{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['inserted_timestamp::DATE'],
    unique_key = "CONCAT_WS('-', tx_id, swap_index)",
    tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}
{# Parse aggregator log events #}
WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_events') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
parse_swap_log AS (
    SELECT
        block_height,
        block_timestamp,
        tx_id,
        event_type,
        ROW_NUMBER() over (
            PARTITION BY tx_id
            ORDER BY
                event_index
        ) - 1 AS swap_index,
        event_data :poolAddress :: STRING AS pool_address,
        event_data :poolSource :: STRING AS pool_source,
        event_data :tokenInAmount :: FLOAT AS token_in_amount,
        event_data :tokenInKey :: STRING AS token_in_key,
        event_data :tokenOutAmount :: FLOAT AS token_out_amount,
        event_data :tokenOutKey :: STRING AS token_out_key
    FROM
        events
    WHERE
        event_contract = 'A.e876e00638d54e75.LogEntry'
        AND event_type = 'PoolSwapInAggregator'
        AND block_height >= 67100587
)
SELECT
    block_height,
    block_timestamp,
    tx_id,
    swap_index,
    pool_address,
    pool_source,
    token_in_amount,
    token_in_key,
    token_out_amount,
    token_out_key,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','swap_index']
    ) }} AS swap_log_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    parse_swap_log
