{{ config(
    severity = 'error',
    tags = ['flow_gap_test'],
    error_if= ">=100"
) }}

WITH results_expected AS (

    SELECT
        block_number AS block_height,
        SUM(tx_count) AS txs_count,
        ARRAY_AGG(collection_id) AS collections_expected,
        array_union_agg(transaction_ids) AS txs_expected,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('silver__streamline_collections') }}

        {% if var(
                'TEST_RANGE',
                False
            ) %}
        WHERE
            block_height BETWEEN {{ var('START_HEIGHT', Null) }}
            AND {{ var('END_HEIGHT', Null) }}
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
        ) AS txs_actual,
        MAX(_inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('silver__streamline_transaction_results') }}

        {% if var(
                'TEST_RANGE',
                False
            ) %}
        WHERE
            block_height BETWEEN {{ var('START_HEIGHT', Null) }}
            AND {{ var('END_HEIGHT', Null) }}
        {% endif %}
    GROUP BY
        1
)
SELECT
    e.block_height,
    e.txs_count AS expected,
    COALESCE(
        A.txs_count,
        0
    ) AS actual,
    expected - actual AS difference,
    silver.udf_array_disjunctive_union(
        e.txs_expected,
        COALESCE(
            A.txs_actual,
            ARRAY_CONSTRUCT()
        )
    ) AS txs_missing,
    A._inserted_timestamp AS _inserted_timestamp
FROM
    results_expected e
    LEFT JOIN results_actual A USING(block_height)
WHERE
    expected != actual
    AND A._inserted_timestamp <= SYSDATE() - INTERVAL '1 day'
ORDER BY
    1
