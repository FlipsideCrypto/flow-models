{{ config(
    materialized = 'table',
    unique_key = 'tx_id'
) }}

WITH txs AS (

    SELECT
        DISTINCT tx_id,
        block_height
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        tx_succeeded = TRUE
),
events AS (
    SELECT
        DISTINCT tx_id,
        block_height
    FROM
        {{ ref('core__fact_events') }}
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
    *,
    row_number() over (order by _inserted_timestamp desc, block_height desc) as sample_index
FROM
    {{ ref('silver__transactions') }}
WHERE
    tx_id IN (
        SELECT
            tx_id
        FROM
            diff
    )
