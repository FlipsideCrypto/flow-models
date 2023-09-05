{{ config(
    severity = 'error',
    tags = ['streamline_test']
) }}

WITH collections_expected AS (

    SELECT
        block_height,
        collection_count
    FROM
        {{ ref('silver__streamline_blocks') }}

        {% if var(
                'TEST_RANGE',
                False
            ) %}
        WHERE
            block_height BETWEEN {{ var('start_height') }}
            AND {{ var('end_height') }}
        {% endif %}
),
collections_actual AS (
    SELECT
        block_number AS block_height,
        COUNT(
            DISTINCT collection_id
        ) AS collection_count
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
    a.collection_count AS actual,
    e.collection_count - a.collection_count AS difference
FROM
    collections_expected e
    JOIN collections_actual a USING(block_height)
WHERE
    e.collection_count != a.collection_count
