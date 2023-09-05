{{ config(
    severity = 'error',
    tags = ['streamline_test']
) }}

WITH collections_expected AS (

    SELECT
        block_height,
        collection_count,
        ARRAY_AGG(
            VALUE :collection_id :: STRING
        ) AS collections_expected
    FROM
        {{ ref('silver__streamline_blocks') }},
        LATERAL FLATTEN(collection_guarantees) {% if var(
                'TEST_RANGE',
                False
            ) %}
        WHERE
            block_height BETWEEN {{ var('start_height') }}
            AND {{ var('end_height') }}
        {% endif %}
    GROUP BY
        1,
        2
),
collections_actual AS (
    SELECT
        block_number AS block_height,
        COUNT(
            DISTINCT collection_id
        ) AS collection_count,
        ARRAY_AGG(collection_id) AS collections_actual
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
)
SELECT
    e.block_height,
    e.collection_count AS expected,
    COALESCE(
        A.collection_count,
        0
    ) AS actual,
    expected - actual AS difference,
    SILVER.UDF_ARRAY_DISJUNCTIVE_UNION(
        e.collections_expected,
        COALESCE(
            A.collections_actual,
            ARRAY_CONSTRUCT()
        )
    ) AS missing_collections
FROM
    collections_expected e
    JOIN collections_actual A USING(block_height)
WHERE
    expected != actual
ORDER BY
    1
