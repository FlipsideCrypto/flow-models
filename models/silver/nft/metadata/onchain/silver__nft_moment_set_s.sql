{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp'],
    unique_key = "concat_ws('-', event_contract, set_id)",
    incremental_strategy = 'delete+insert',
    tags = ['nft', 'dapper', 'scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH events AS (

    SELECT
        block_height,
        block_timestamp,
        tx_id,
        event_id,
        event_index,
        event_type,
        event_contract,
        event_data,
        _inserted_timestamp,
        _partition_by_block_id,
        modified_timestamp
    FROM
        {{ ref('silver__streamline_events') }}
    WHERE
        event_type = 'SetCreated'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    tx_id,
    event_id,
    block_timestamp,
    event_contract,
    event_data AS set_data,
    COALESCE(
        event_data :id :: STRING,
        event_data :setID :: STRING
    ) AS set_id,
    event_data :name :: STRING AS set_name,
    event_data :series :: STRING AS series_id,
    _inserted_timestamp,
    _partition_by_block_id,
    {{ dbt_utils.generate_surrogate_key(
        ['event_id']
    ) }} AS nft_moment_set_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    events
WHERE
    set_id IS NOT NULL
