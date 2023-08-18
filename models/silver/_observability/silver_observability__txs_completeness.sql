{{ config(
    materialized = 'incremental',
    unique_key = '_test_timestamp',
    enabled = False
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
        -- TEMP FILTER FOR TESTING API
        block_height BETWEEN 55000000 and 55000100
        {# block_timestamp <= DATEADD('hour', -12, SYSDATE())

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
    {% endif %} #}
{# )
{% endif %} #}
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
        block_height BETWEEN (
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
txs_per_block_actual as (
    select
        block_height,
        count(distinct tx_id) as txs
    from {{ ref('silver__transactions') }}
    where block_height in (
        select block_height from block_range
    )
    group by 1
),
params as (
    select
        block_height,
        'query ($network: FlowNetwork!, $height: Int!) {
            flow(network: $network) {
                blocks(
                height: {is: $height}
                ) {
                height
                transactionsCount
                collectionsCount
                }
            }
            }' as query,
            {'network':'flow', 'height': block_height} as variables,
            'BQYgI4k947QztRIx9FrfpjXq7u4cnPRh' as api_key
    from block_range
),
txs_per_block_expected as (
    select
        block_height,
        livequery_dev.live.udf_api(
            'POST',
            'https://graphql.bitquery.io',
            {
                'Content-Type': 'application/json',
                'X-API-KEY': api_key
            },
            object_construct('query', query, 'variables', variables)
        ) as res,
        res:data:data:flow:blocks:transactionsCount as txs,
        res:data:data:flow:blocks:height as block_height_res

    from params
),
comparison as (
    select
        id.block_height,
        a.txs as actual_tx_count,
        e.txs as expected_tx_count
    from block_range id
    left join txs_per_block_actual a using (block_height)
    left join txs_per_block_expected e using (block_height)
)
select * from comparison
