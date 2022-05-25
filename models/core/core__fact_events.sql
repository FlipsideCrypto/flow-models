{{ config(
    materialized = 'view'
) }}

WITH events_final AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        block_timestamp >= '2022-05-09'
),
events AS (
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
        events_final
)
SELECT
    *
FROM
    events
