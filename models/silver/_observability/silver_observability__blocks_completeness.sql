{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = False
) }}

WITH summary_stats AS (

    SELECT
        MIN(block_height) AS min_block,
        MAX(block_height) AS max_block,
        MIN(block_timestamp) AS min_block_timestamp,
        MAX(block_timestamp) AS max_block_timestamp,
        COUNT(1) AS blocks_tested
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_timestamp <= DATEADD('hour', -12, SYSDATE())

{% if is_incremental() %}
AND (
    block_height >= (
        SELECT
            MIN(block_height)
        FROM
            (
                SELECT
                    MIN(block_height) AS block_height
                FROM
                    {{ ref('silver__blocks') }}
                WHERE
                    block_timestamp BETWEEN DATEADD('hour', -96, SYSDATE())
                    AND DATEADD('hour', -95, SYSDATE())
                UNION
                SELECT
                    MIN(VALUE) - 1 AS block_height
                FROM
                    (
                        SELECT
                            blocks_impacted_array
                        FROM
                            {{ this }}
                            qualify ROW_NUMBER() over (
                                ORDER BY
                                    test_timestamp DESC
                            ) = 1
                    ),
                    LATERAL FLATTEN(
                        input => blocks_impacted_array
                    )
            )
    ) {% if var('OBSERV_FULL_TEST') %}
        OR block_height >= 7601063
    {% endif %}
)
{% endif %}
),
block_range AS (
    SELECT
        _id AS block_height
    FROM
        {{ source(
            'crosschain_silver',
            'number_sequence'
        ) }}
    WHERE
        _id BETWEEN (
            SELECT
                min_block
            FROM
                summary_stats
        )
        AND (
            SELECT
                max_block
            FROM
                summary_stats
        )
),
blocks AS (
    SELECT
        l.block_height,
        block_timestamp,
        LAG(
            l.block_height,
            1
        ) over (
            ORDER BY
                l.block_height ASC
        ) AS prev_BLOCK_HEIGHT
    FROM
        {{ ref("silver__blocks") }}
        l
        INNER JOIN block_range b
        ON l.block_height = b.block_height
        AND l.block_height >= (
            SELECT
                MIN(block_height)
            FROM
                block_range
        )
),
block_gen AS (
    SELECT
        _id AS block_height
    FROM
        {{ source(
            'crosschain_silver',
            'number_sequence'
        ) }}
    WHERE
        _id BETWEEN (
            SELECT
                MIN(block_height)
            FROM
                blocks
        )
        AND (
            SELECT
                MAX(block_height)
            FROM
                blocks
        )
),
test_blocks AS (
    SELECT
        'blocks' AS test_name,
        MIN(
            b.block_height
        ) AS min_block,
        MAX(
            b.block_height
        ) AS max_block,
        MIN(
            b.block_timestamp
        ) AS min_block_timestamp,
        MAX(
            b.block_timestamp
        ) AS max_block_timestamp,
        COUNT(1) AS blocks_tested,
        COUNT(
            CASE
                WHEN C.block_height IS NOT NULL THEN A.block_height
            END
        ) AS blocks_impacted_count,
        ARRAY_AGG(
            CASE
                WHEN C.block_height IS NOT NULL THEN A.block_height
            END
        ) within GROUP (
            ORDER BY
                A.block_height
        ) AS blocks_impacted_array,
        SYSDATE() AS test_timestamp
    FROM
        block_gen A
        LEFT JOIN blocks b
        ON A.block_height = b.block_height
        LEFT JOIN blocks C
        ON A.block_height > C.prev_block_height
        AND A.block_height < C.block_height
        AND C.block_height - C.prev_block_height <> 1
    WHERE
        COALESCE(
            b.block_height,
            C.block_height
        ) IS NOT NULL
),
FINAL AS (
    SELECT
        test_name,
        min_block,
        max_block,
        min_block_timestamp,
        max_block_timestamp,
        blocks_tested,
        blocks_impacted_count,
        blocks_impacted_array,
        test_timestamp
    FROM
        test_blocks
)
SELECT
    *
FROM
    FINAL
