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

moments_to_fetch AS (
    SELECT
        m.event_contract,
        m.moment_id
    FROM
        {{ ref('livequery__topshot_moments_metadata_needed') }} m
    LEFT JOIN (
        SELECT
            nft_id AS moment_id,
            MAX(price) AS max_price
        FROM
            {{ ref('nft__ez_nft_sales') }}
        WHERE
            nft_collection = 'A.0b2a3299cc857e29.TopShot'
        GROUP BY
            moment_id
        ORDER BY
            max_price DESC
        LIMIT 500
    ) s
    ON m.moment_id = s.moment_id
    ORDER BY
        s.max_price DESC NULLS LAST
    LIMIT 100 
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

api_responses AS (
    SELECT
        event_contract,
        moment_id,
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

SELECT
    event_contract,
    moment_id,
    data,
    _inserted_date,
    _inserted_timestamp,
    _res_id
FROM
    api_responses