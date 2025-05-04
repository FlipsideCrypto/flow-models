{{ config(
    materialized = 'view',
    tags = ['streamline', 'topshot', 'moments_metadata', 'backfill']
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
            AND block_timestamp::DATE = '2024-09-06'::DATE  
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
    m.event_contract AS contract,
    m.moment_id AS id,
    p.base_url,
    OBJECT_CONSTRUCT(
        'query', p.query,
        'variables', OBJECT_CONSTRUCT('momentId', m.moment_id)
    ) AS payload,
    (SELECT last_date FROM last_metadata_date) AS last_metadata_date  -- For reference only
FROM
    moments_to_backfill m
    CROSS JOIN api_parameters p