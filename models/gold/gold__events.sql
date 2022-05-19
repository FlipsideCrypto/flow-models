{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::date'],
    unique_key = "CONCAT_WS('-', tx_id, event_index)"
) }}

WITH silver_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        block_timestamp >= '2022-05-09'

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE -2
{% endif %}
),
silver_event_attributes AS (
    SELECT
        *
    FROM
        {{ ref('silver__event_attributes') }}
    WHERE
        block_timestamp >= '2022-05-09'

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE -2
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
        ) AS event_data
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
        A.event_data
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
        event_data
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
        event_data
    FROM
        location_object
)
SELECT
    *
FROM
    FINAL
