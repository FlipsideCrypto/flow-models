{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['inserted_timestamp::DATE'],
    unique_key = 'swap_log_id',
    tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH events AS (

    SELECT
        block_height,
        block_timestamp,
        tx_id,
        event_index,
        event_contract,
        event_type,
        event_data,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
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
        event_data :tokenInKey :: STRING AS token_in_contract,
        event_data :tokenOutAmount :: FLOAT AS token_out_amount,
        event_data :tokenOutKey :: STRING AS token_out_contract
    FROM
        events
    WHERE
        event_contract = 'A.e876e00638d54e75.LogEntry'
        AND event_type = 'PoolSwapInAggregator'
        AND block_height >= 67100587
),
transactions AS (
    SELECT
        tx_id,
        authorizers
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        tx_id IN (
            SELECT
                DISTINCT tx_id
            FROM
                parse_swap_log
        )
        AND block_height >= 67100587

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_height,
    block_timestamp,
    s.tx_id,
    swap_index,
    pool_address,
    pool_source,
    t.authorizers [0] :: STRING AS trader,
    token_in_amount,
    token_in_contract,
    token_out_amount,
    token_out_contract,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','swap_index']
    ) }} AS swap_log_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    parse_swap_log s
    LEFT JOIN transactions t USING (tx_id)
