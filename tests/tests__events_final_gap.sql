{{ config(
    severity = 'error',
    tags = ['streamline_test']
) }}

WITH txs AS (

    SELECT
        block_height,
        tx_id,
        ARRAY_SIZE(events) AS event_ct
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        block_height >= {{ var('STREAMLINE_START_BLOCK') }}
        AND (_inserted_timestamp BETWEEN SYSDATE() - INTERVAL '3 days'
            AND SYSDATE() - INTERVAL '2 hours')
),
events AS (
    SELECT
        block_height,
        tx_id,
        COUNT(1) AS event_ct
    FROM
        {{ ref('silver__streamline_events') }}
    WHERE
        block_height >= {{ var('STREAMLINE_START_BLOCK') }}
        AND(_inserted_timestamp BETWEEN SYSDATE() - INTERVAL '3 days'
        AND SYSDATE() - INTERVAL '2 hours')
    GROUP BY
        1,
        2
),
compare AS (
        SELECT
            txs.block_height,
            txs.tx_id,
            txs.event_ct AS tx_event_ct,
            events.event_ct AS event_event_ct
        FROM
            txs
            LEFT JOIN events
            ON txs.block_height = events.block_height
            AND txs.tx_id = events.tx_id
        WHERE
            txs.event_ct != events.event_ct
    )
SELECT
    *
FROM
    compare
