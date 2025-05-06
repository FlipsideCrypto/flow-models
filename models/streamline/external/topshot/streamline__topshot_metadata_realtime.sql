{{ config(
    materialized = 'view',
    tags = ['streamline', 'topshot', 'moments_metadata', 'backfill'],
    post_hook = fsc_utils.if_data_call_function_v2(
        func = '{{this.schema}}.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table": "topshot_metadata",
        "sql_limit": "100",
        "producer_batch_size": "100",
        "worker_batch_size": "100",
        "sql_source": "{{this.identifier}}",
        "async_concurrent_requests": "10" }
    )
) }}

WITH api_parameters AS (
    -- Use the same parameters as for realtime

    SELECT
        base_url,
        query
    FROM
        {{ ref('streamline__topshot_parameters') }}
    WHERE
        contract = 'A.0b2a3299cc857e29.TopShot'
),
work_todo AS (
    SELECT
        event_contract,
        moment_id
    FROM
        {{ ref('streamline__topshot_moments') }}
    EXCEPT
    SELECT
        event_contract,
        moment_id
    FROM
        {{ ref('streamline__topshot_metadata_complete') }}
)
SELECT
    to_char(TO_TIMESTAMP_NTZ(SYSDATE()), 'YYYYMMDD') AS partition_key,
    m.event_contract AS contract,
    m.moment_id AS id,
    {{ target.database }}.live.udf_api(
        'POST',
        p.base_url,
        OBJECT_CONSTRUCT(
            'Accept',
            'application/json',
            'Accept-Encoding',
            'gzip',
            'Connection',
            'keep-alive',
            'Content-Type',
            'application/json',
            'User-Agent',
            'Flipside_Flow_metadata/0.1'
        ),
        OBJECT_CONSTRUCT(
            'query',
            p.query,
            'variables',
            OBJECT_CONSTRUCT(
                'momentId',
                m.moment_id
            )
        )
    ) AS request
FROM
    work_todo m
    CROSS JOIN api_parameters p
