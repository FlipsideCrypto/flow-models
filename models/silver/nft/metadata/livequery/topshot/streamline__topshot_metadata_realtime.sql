{{ config (
    materialized = "view",
    tags = ['streamline', 'topshot', 'moment_metadata'],
    post_hook = fsc_utils.if_data_call_function_v2(
        func = '{{this.schema}}.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table": "moments_minted_metadata_api",
        "sql_limit": "50",
        "producer_batch_size": "100",
        "worker_batch_size": "100",
        "sql_source": "{{this.identifier}}",
        "async_concurrent_requests": "5" }
    ),
    enabled = false
) }}

WITH api_parameters AS (
    -- Use the same parameters as the LiveQuery model

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
        {{ ref('livequery__topshot_moments_metadata_needed') }}
        m
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
                DISTINCT nft_id AS moment_id,
                block_timestamp
            FROM
                {{ ref('nft__ez_nft_sales') }}
            WHERE
                nft_collection = 'A.0b2a3299cc857e29.TopShot'
                AND block_timestamp >= DATEADD(DAY, -7, CURRENT_DATE()) -- Filter to only fetch metadata for moments that had activity in the last 7 days) recent_txs
                ON m.moment_id = recent_txs.moment_id
            WHERE
                COALESCE(
                    null_attempts.failure_count,
                    0
                ) < 3
                AND recent_txs.moment_id IS NOT NULL -- Only include moments with recent transactions
            ORDER BY
                recent_txs.block_timestamp DESC
            LIMIT
                {{ var(
                    'SQL_LIMIT', 50
                ) }}
        )
    SELECT
        DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
        m.event_contract AS contract,
        m.moment_id AS id,
        {{ target.database }}.live.udf_api(
            'POST',
            p.base_url,{ 'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip',
            'Connection': 'keep-alive',
            'User-Agent': 'Flipside_Flow_metadata/0.1' },{ 'query': p.query,
            'variables':{ 'momentId': m.moment_id }},
            NULL
        ) AS request
    FROM
        moments_to_fetch m
        CROSS JOIN api_parameters p
