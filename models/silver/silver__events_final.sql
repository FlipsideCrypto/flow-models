{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::date'],
    unique_key = "CONCAT_WS('-', tx_id, event_index)"
) }}

WITH silver_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}

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
silver_event_attributes AS (
    SELECT
        *
    FROM
        {{ ref('silver__event_attributes') }}

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
objs AS (
    SELECT
        event_id,
        OBJECT_AGG(
            attribute_key,
            attribute_value_adj :: variant
        ) AS event_data
    FROM
        silver_event_attributes
    GROUP BY
        1
),
location_object AS (
    SELECT
        event_id,
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_index,
        event_contract,
        event_type,
        COALESCE(
            _event_data_type :location,
            _event_data_type :Location
        ) AS event_data,
        _ingested_at,
        _inserted_timestamp
    FROM
        silver_events
    WHERE
        _event_data_fields = '[]'
),
gold_events AS (
    SELECT
        e.event_id,
        e.tx_id,
        e.block_timestamp,
        e.block_height,
        e.tx_succeeded,
        e.event_index,
        e.event_contract,
        e.event_type,
        A.event_data,
        e._ingested_at,
        e._inserted_timestamp
    FROM
        objs A
        LEFT JOIN silver_events e USING (event_id)
),
FINAL AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_index,
        event_contract,
        event_type,
        event_data,
        _ingested_at,
        _inserted_timestamp
    FROM
        gold_events
    UNION
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_index,
        event_contract,
        event_type,
        event_data,
        _ingested_at,
        _inserted_timestamp
    FROM
        location_object
)
SELECT
    *
FROM
    FINAL
