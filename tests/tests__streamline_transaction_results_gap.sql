{{ config(
    severity = 'error',
    tags = ['streamline_test']
) }}

WITH results_expected AS (

    SELECT
        block_number AS block_height,
        SUM(tx_count) AS txs_count,
        ARRAY_AGG(collection_id) AS collections_expected,
        ARRAY_UNION_AGG(transaction_ids) AS txs_expected
    FROM
        {{ ref('silver__streamline_collections') }}

        {% if var(
                'TEST_RANGE',
                False
            ) %}
        WHERE
            block_height BETWEEN {{ var('start_height') }}
            AND {{ var('end_height') }}
        {% endif %}
    GROUP BY
        1
),
results_actual AS (
    SELECT
        block_number AS block_height,
        COUNT(
            DISTINCT tx_id
        ) AS txs_count,
        ARRAY_AGG(
            DISTINCT tx_id
        ) AS txs_actual
    FROM
        {{ ref('silver__streamline_transaction_results') }}

        {% if var(
                'TEST_RANGE',
                False
            ) %}
        WHERE
            block_height BETWEEN {{ var('start_height') }}
            AND {{ var('end_height') }}
        {% endif %}
    GROUP BY
        1
)
SELECT
    e.block_height,
    e.txs_count AS expected,
    A.txs_count AS actual,
    e.txs_count - A.txs_count AS difference,
    SILVER.UDF_ARRAY_DISJUNCTIVE_UNION(
        e.txs_expected,
        A.txs_actual
    ) AS txs_missing
FROM
    results_expected e
    JOIN results_actual A USING(block_height)
WHERE
    e.txs_count != A.txs_count
