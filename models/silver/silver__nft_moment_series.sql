{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp'],
    unique_key = "concat_ws('-', event_contract, series_id)",
    incremental_strategy = 'delete+insert',
    tags = ['nft', 'dapper', 'nft-metadata']
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        event_type = 'SeriesCreated'
        AND ARRAY_CONTAINS('name' :: variant, object_keys(event_data))

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
org AS (
    SELECT
        tx_id,
        block_timestamp,
        event_contract,
        event_data :id :: STRING AS series_id,
        event_data :name :: STRING AS series_name,
        _inserted_timestamp
    FROM
        events
)
SELECT
    *
FROM
    org
