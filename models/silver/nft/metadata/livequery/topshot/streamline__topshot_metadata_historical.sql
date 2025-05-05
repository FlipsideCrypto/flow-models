{{ config(
    materialized = 'view',
    tags = ['streamline', 'topshot', 'moments_metadata', 'backfill'],
    post_hook = fsc_utils.if_data_call_function_v2(
        func = '{{this.schema}}.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "moments_minted_metadata_api",
            "sql_limit": "100",
            "producer_batch_size": "100",
            "worker_batch_size": "100",
            "sql_source": "{{this.identifier}}",
            "async_concurrent_requests": "10"
        }
    )
) }}

WITH api_parameters AS (
    -- Use the same parameters as for realtime
    SELECT
        base_url,
        query
    FROM
        {{ ref('livequery__moments_parameters_new') }}
    WHERE
        contract = 'A.0b2a3299cc857e29.TopShot'
),

-- Get the most recent date we have metadata for
last_metadata_date AS (
    SELECT
        MAX(_inserted_timestamp)::DATE AS last_date
    FROM
        {{ ref('silver__nft_topshot_metadata') }}
),

-- Find all historical moments that need metadata
moments_to_backfill AS (
    SELECT
        m.event_contract,
        m.moment_id
    FROM
        {{ ref('livequery__topshot_moments_metadata_needed') }} m
    LEFT JOIN (
        SELECT 
            moment_id,
            COUNT(*) AS failure_count
        FROM 
            {{ ref('livequery__null_moments_metadata') }}
        GROUP BY 
            moment_id
    ) null_attempts
    ON m.moment_id = null_attempts.moment_id
    LEFT JOIN (
        SELECT
            event_data:momentID::STRING AS moment_id,
            block_timestamp
        FROM
            {{ ref('silver__nft_moments_s') }}
        WHERE
            event_contract = 'A.0b2a3299cc857e29.TopShot'
            AND event_type = 'MomentMinted'
            AND block_timestamp::DATE >= '2024-09-06'::DATE  
    ) mint_data
    ON m.moment_id = mint_data.moment_id
    WHERE
        COALESCE(null_attempts.failure_count, 0) < 3

    ORDER BY
        mint_data.block_timestamp ASC NULLS LAST,
        CAST(m.moment_id AS INTEGER) ASC
     LIMIT {{ var('SQL_LIMIT', 100) }} 
)

SELECT
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    m.event_contract AS contract,
    m.moment_id AS id,
    {{ target.database }}.live.udf_api(
        'POST',
        p.base_url,
        {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip',
            'Connection': 'keep-alive',
            'User-Agent': 'Flipside_Flow_metadata/0.1'
        },
        {
            'query': p.query,
            'variables': {'momentId': m.moment_id}
        },
        NULL
    ) AS request
FROM
    moments_to_backfill m
    CROSS JOIN api_parameters p