{{ config(
    severity = 'error',
    tags = ['streamline_test']
) }}

WITH streamline_blocks AS (

    SELECT
        *
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
determine_prior_block AS (
    SELECT
        block_height,
        block_id,
        parent_id,
        LAG(block_id) over (
            ORDER BY
                block_height
        ) AS prev_block_id,
        LAG(block_height) over (
            ORDER BY
                block_height
        ) AS prev_block_height
    FROM
        streamline_blocks
)
SELECT
    *
FROM
    determine_prior_block
WHERE
    prev_block_id != parent_id
