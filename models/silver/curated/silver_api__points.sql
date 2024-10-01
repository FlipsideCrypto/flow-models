{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_rest_api_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'POINTS_API', 'sql_limit', {{var('sql_limit','50000')}}, 'producer_batch_size', {{var('producer_batch_size','50000')}}, 'worker_batch_size', {{var('worker_batch_size','25000')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_non_core']
) }}


with
get_addresses AS (
    -- note returns 50
    SELECT
        flow.live.udf_api(
            'GET',
            'https://evm-testnet.flowscan.io/api/v2/addresses',
            {},
            {}
        ) as result
),
address_list AS (
    select
        VALUE :hash :: STRING as address
    from get_addresses, lateral flatten (result:data:items :: ARRAY)
    LIMIT 5
)
SELECT
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    address,
    flow.live.udf_api(
        'GET',
        'https://crescendo-rewards-c1-309975214470.us-central1.run.app/points/ethereum/' || address,
        {},
        {}
    ) AS result
FROM address_list

