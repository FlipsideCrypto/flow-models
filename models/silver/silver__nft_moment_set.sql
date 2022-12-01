{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp'],
    unique_key = "concat_ws('-', event_contract, set_id)",
    incremental_strategy = 'delete+insert',
    tags = ['nft', 'dapper']
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        event_type = 'SetCreated'
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
        event_data :id :: STRING AS set_id,
        event_data :name :: STRING AS set_name,
        _inserted_timestamp
    FROM
        events
    WHERE
        set_id IS NOT NULL
)
SELECT
    *
FROM
    org
