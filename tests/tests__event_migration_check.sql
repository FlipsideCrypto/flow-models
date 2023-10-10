{{ config(
    enabled = False
) }}

WITH dev_sample AS (

    SELECT
        *
    FROM
        flow_dev.silver.streamline_events
    WHERE
        block_height >= 55114467
),
prod_sample AS (
    SELECT
        *
    FROM
        flow.silver.events_final
    WHERE
        block_height >= 55114467
),
dcheck AS (
    SELECT
        d.tx_id,
        d.block_height,
        d.event_id,
        d.event_contract = p.event_contract AS event_contract_match,
        d.event_type = p.event_type AS event_type_match,
        d.event_data AS event_data_s,
        p.event_data AS event_data_cw,
        d.event_data = p.event_data AS event_data_match
    FROM
        dev_sample d
        LEFT JOIN prod_sample p USING (
            tx_id,
            event_index
        )
    WHERE
        p.tx_id IS NOT NULL
)
SELECT
    *
FROM
    dcheck
WHERE
    NOT event_data_match
