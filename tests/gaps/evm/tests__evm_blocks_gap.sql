{{ config(
    tags = ['evm_gap_test'],
    severity = 'error'
) }}

WITH MIN AS (

    SELECT
        MIN(
            block_timestamp :: DATE
        ) bd
    FROM
        {{ ref('silver_evm__blocks') }}

        {% if not var(
                'DBT_TEST_FULL',
                False
            ) %}
        WHERE
            _inserted_timestamp >= SYSDATE() - INTERVAL '7 days'
        {% endif %}
),
blocks AS (
    SELECT
        block_number,
        block_hash,
        parent_hash,
        _inserted_timestamp
    FROM
        {{ ref('silver_evm__blocks') }}
    WHERE
        block_timestamp < DATEADD('hour', -3, SYSDATE())
        AND block_timestamp :: DATE >= (
            SELECT
                bd
            FROM
                MIN
        )
),
check_orphan AS (
    SELECT
        child.block_number,
        child.block_hash,
        child.parent_hash,
        child._inserted_timestamp,
        PARENT.block_number AS parent_block_number,
        PARENT.block_hash AS confirmed_parent_hash
    FROM
        blocks child
        LEFT JOIN blocks PARENT
        ON child.parent_hash = PARENT.block_hash
    ORDER BY
        block_number
),
determine_previous_block AS (
    SELECT
        block_number,
        block_hash,
        parent_hash,
        confirmed_parent_hash,
        _inserted_timestamp,
        LAG(block_number) over (
            ORDER BY
                block_number
        ) AS prev_block_number,
        ROW_NUMBER() over(
            ORDER BY
                block_number
        ) rn
    FROM
        check_orphan
)
SELECT
    block_number,
    block_hash,
    parent_hash,
    prev_block_number,
    block_number - prev_block_number AS gap_size,
    _inserted_timestamp
FROM
    determine_previous_block
WHERE
    confirmed_parent_hash IS NULL
    AND block_number > 1 -- may be some temporarily missing blocks at chainhead, only issue if not filled on subsequent run
    AND rn > 1
    AND _inserted_timestamp <= SYSDATE() - INTERVAL '1 hour'
