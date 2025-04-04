{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = '{{this.schema}}.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "reward_points",
            "sql_limit": "50000",
            "producer_batch_size": "3000",
            "worker_batch_size": "1500",
            "sql_source": "{{this.identifier}}"
        }
    )
) }}

WITH evm_addresses AS (

    SELECT
        DISTINCT address AS address
    FROM
        {{ ref('streamline__evm_addresses') }}
    WHERE
        address IS NOT null
),
complete_addresses AS (
    SELECT
        DISTINCT address
    FROM
        {{ ref('streamline__reward_points_complete') }}
    WHERE
        request_date = SYSDATE() :: DATE
),
addresses_to_query AS (
    SELECT
        a.address
    FROM
        evm_addresses a
    LEFT JOIN complete_addresses b on a.address = b.address
    WHERE 
        b.address is null       
)
SELECT
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    address,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/points/ethereum/' || address,
        {
            'User-Agent': 'Flipside/0.1',
            'Accept': 'application/json',
            'Connection': 'keep-alive'
        },
        {},
        'Vault/prod/flow/points-api/prod'
    ) AS request
FROM
    addresses_to_query
