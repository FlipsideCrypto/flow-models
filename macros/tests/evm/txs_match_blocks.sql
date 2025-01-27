{% test txs_match_blocks(
    model,
    blocks_model
) %}
WITH count_txs AS (
    SELECT
        block_number,
        COUNT(*) AS record_count
    FROM
        {{ model }}
    GROUP BY
        ALL
),
block_txs AS (
    SELECT
        block_number,
        tx_count AS expected_count
    FROM
        {{ blocks_model }}
)
SELECT
    block_number,
    record_count AS actual_count,
    expected_count
FROM
    block_txs
    LEFT JOIN count_txs USING (block_number)
WHERE
    record_count != expected_count
    OR expected_count IS NULL 
{% endtest %}