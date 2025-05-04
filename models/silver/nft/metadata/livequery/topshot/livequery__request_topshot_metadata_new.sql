{{ config(
    materialized = 'incremental',
    unique_key = '_RES_ID',
    tags = ['livequery', 'topshot', 'moment_metadata'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_INSERTED_TIMESTAMP'],
    full_refresh = False
) }}

WITH api_parameters AS (
    SELECT
        base_url,
        query
    FROM
        {{ ref('livequery__moments_parameters') }}
    WHERE
        contract = 'A.0b2a3299cc857e29.TopShot'
),

-- Find all moments that need metadata - focusing on post-August 2024 moments first
moments_to_fetch AS (
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
        SELECT DISTINCT
            nft_id AS moment_id,
            block_timestamp
        FROM
            {{ ref('nft__ez_nft_sales') }}
        WHERE
            nft_collection = 'A.0b2a3299cc857e29.TopShot'
            AND block_timestamp >= '2024-08-01'
    ) recent_txs
    ON m.moment_id = recent_txs.moment_id
    WHERE
        COALESCE(null_attempts.failure_count, 0) < 3
    ORDER BY
        CASE WHEN recent_txs.moment_id IS NOT NULL THEN 0 ELSE 1 END,
        recent_txs.block_timestamp DESC NULLS LAST
    LIMIT 100 -- Process in batches to respect rate limits
),

api_calls AS (
    SELECT
        p.base_url,
        p.query,
        m.event_contract,
        m.moment_id,
        OBJECT_CONSTRUCT(
            'Accept', 'application/json',
            'Accept-Encoding', 'gzip',
            'Connection', 'keep-alive',
            'Content-Type', 'application/json',
            'User-Agent', 'Flipside_Flow_metadata/0.1'
        ) AS headers,
        OBJECT_CONSTRUCT(
            'query', p.query,
            'variables', OBJECT_CONSTRUCT('momentId', m.moment_id)
        ) AS payload
    FROM
        api_parameters p
        CROSS JOIN moments_to_fetch m
),

-- Execute the API calls using the UDF framework
api_responses AS (
    SELECT
        event_contract,
        moment_id,
        -- Call the UDF_API function with the prepared parameters
        flow.live.udf_api(
            'POST',
            base_url,
            headers,
            payload
        ) AS data,
        SYSDATE() AS _inserted_date,
        SYSDATE() AS _inserted_timestamp,
        MD5(event_contract || moment_id) AS _res_id
    FROM
        api_calls
)

-- Return the results
SELECT
    event_contract,
    moment_id,
    data,
    _inserted_date,
    _inserted_timestamp,
    _res_id
FROM
    api_responses