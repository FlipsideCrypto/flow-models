{{ config(
    error_if = '>1',
    tags = ['flow_gap_test']
) }}

WITH streamline_blocks AS (

    SELECT
        block_number,
        id,
        parent_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__streamline_blocks') }}

        {% if not var(
                'DBT_TEST_FULL',
                False
            ) %}
        WHERE
            _inserted_timestamp >= SYSDATE() - INTERVAL '7 days'
        {% endif %}
),
check_orphan AS (
    SELECT
        child.block_number,
        child.id,
        child.parent_id,
        child._inserted_timestamp,
        PARENT.block_number AS parent_block_number,
        PARENT.id AS confirmed_parent_id
    FROM
        streamline_blocks child
        LEFT JOIN streamline_blocks PARENT
        ON child.parent_id = PARENT.id
    ORDER BY
        block_number
),
determine_previous_block AS (
    SELECT
        block_number,
        id,
        parent_id,
        confirmed_parent_id,
        _inserted_timestamp,
        LAG(block_number) over (
            ORDER BY
                block_number
        ) AS prev_block_number
    FROM
        check_orphan
)
SELECT
    block_number,
    id,
    parent_id,
    prev_block_number,
    block_number - prev_block_number AS gap_size,
    _inserted_timestamp
FROM
    determine_previous_block
WHERE
    confirmed_parent_id IS NULL
    AND block_number > 4132134 -- mainnet genesis
