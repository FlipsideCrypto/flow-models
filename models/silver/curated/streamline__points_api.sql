{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = '{{this.schema}}.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "points_api",
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
        DISTINCT from_address AS address
    FROM
        {{ ref('silver_evm__transactions') }}
        -- note dev just has 3 rn because it hasn't been refreshed
        -- good for testing
)
SELECT
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    flow.live.udf_api(
        'GET',
        'https://crescendo-rewards-c1-309975214470.us-central1.run.app/points/ethereum/' || address,
        {},
        {}
    ) AS request
FROM
    evm_addresses
