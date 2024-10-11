{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = '{{this.schema}}.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "reward_points",
            "sql_limit": "1000",
            "producer_batch_size": "1000",
            "worker_batch_size": "1000",
            "sql_source": "{{this.identifier}}"
        }
    ),
    tags = ['streamline_non_core']
) }}

WITH evm_addresses AS (

    SELECT
        DISTINCT address AS address
    FROM
        {{ ref('streamline__evm_addresses') }}
)
SELECT
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    address,
    flow.live.udf_api(
        'GET',
        '{Service}/points/ethereum/' || address,
        {
            'User-Agent': 'Flipside/0.1',
            'Accept': 'application/json',
            'Connection': 'keep-alive'
        },
        {}
    ) AS request
FROM
    evm_addresses
