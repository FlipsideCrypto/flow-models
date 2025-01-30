{% test events_match_txs(
    model,
    transactions_model
) %}
WITH logs AS (
    SELECT
        DISTINCT block_number,
        tx_hash,
        tx_position
    FROM
        {{ model }}
),
missing_transactions AS (
    SELECT
        logs.block_number,
        logs.tx_hash,
        logs.tx_position
    FROM
        logs
        LEFT JOIN {{ transactions_model }}
        txs USING (
            block_number,
            tx_hash,
            tx_position
        )
    WHERE
        txs.tx_hash IS NULL
        OR txs.tx_position IS NULL
        OR txs.block_number IS NULL
)
SELECT
    *
FROM
    missing_transactions 
{% endtest %}