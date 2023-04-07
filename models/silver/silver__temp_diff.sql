{{ config(
    materialized = 'table',
    unique_key = 'tx_id'
) }}

WITH txs AS (

    SELECT
        DISTINCT tx_id,
        block_height
    FROM
        flow.core.fact_transactions
    WHERE
        tx_succeeded = TRUE
),
events AS (
    SELECT
        DISTINCT tx_id,
        block_height
    FROM
        flow.core.fact_events
    WHERE
        tx_succeeded = TRUE
),
diff AS (
    SELECT
        *
    FROM
        txs
    EXCEPT
    SELECT
        *
    FROM
        events
)
SELECT
    *
FROM
    flow.silver.transactions
WHERE
    tx_id IN (
        SELECT
            tx_id
        FROM
            diff
    )
