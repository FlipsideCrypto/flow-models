{{ config(
    materialized = 'incremental',
    unique_key = 'block_height',
    tags = ['get_block_tx_count', 'observability'],
    full_refresh = False
) }}

{% if var('FIX_GAPS', False) %}
    WITH blocks AS (
        SELECT
            *
        FROM
            {{ this }}
    ),
    determine_prior_block AS (
        SELECT
            block_height,
            LAG(block_height) over (
                ORDER BY
                    block_height
            ) AS prev_block_height
        FROM
            blocks
    ),
    gaps AS (
        SELECT
            block_height,
            prev_block_height,
            block_height - prev_block_height AS gap
        FROM
            determine_prior_block
        WHERE
            gap > 1
        ORDER BY
            1
    ),
    params AS (
        SELECT
            'query ($network: FlowNetwork!, $block_height_start: Int!, $block_height_end: Int!) { flow(network: $network) { blocks(height: {gt: $block_height_start, lteq: $block_height_end}) { height transactionsCount } } }' AS query,
            OBJECT_CONSTRUCT(
                'network',
                'flow',
                'block_height_start',
                prev_block_height,
                'block_height_end',
                (prev_block_height + gap - 1) :: INTEGER
            ) AS variables,
            '{{ var('BITQUERY_API_KEY', Null) }}' AS api_key
        FROM
            gaps
    ),
{% else %}
    WITH starting_block AS (

{% if is_incremental() %}
SELECT
    MAX(block_height) AS block_height_start, {{ target.database }}.streamline.udf_get_chainhead() AS max_block_height
FROM
    {{ this }}
{% else %}
    -- Candidate 4 starts at 4132133
SELECT
    4132133 AS block_height_start, 1000000000 AS max_block_height
{% endif %}),
params AS (
    SELECT
        'query ($network: FlowNetwork!, $block_height_start: Int!, $block_height_end: Int!) { flow(network: $network) { blocks(height: {gt: $block_height_start, lteq: $block_height_end}) { height transactionsCount } } }' AS query,
        OBJECT_CONSTRUCT(
            'network',
            'flow',
            'block_height_start',
            block_height_start,
            'block_height_end',
            IFF(
                block_height_start + 25000 > max_block_height,
                max_block_height - 500,
                block_height_start + 25000
            ) :: INTEGER
        ) AS variables,
        '{{ var('BITQUERY_API_KEY', Null) }}' AS api_key
    FROM
        starting_block
),
{% endif %}

get_bitquery AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'POST',
            'https://graphql.bitquery.io',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json',
                'X-API-KEY',
                api_key
            ),
            OBJECT_CONSTRUCT(
                'query',
                query,
                'variables',
                variables
            )
        ) AS res,
        res :data :data :flow :blocks :: ARRAY AS blocks_res
    FROM
        params
)
SELECT
    VALUE :height :: INTEGER AS block_height,
    VALUE :transactionsCount :: INTEGER AS transaction_ct,
    SYSDATE() AS _inserted_timestamp
FROM
    get_bitquery,
    LATERAL FLATTEN(blocks_res) 
    qualify ROW_NUMBER() over (
        PARTITION BY block_height
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
