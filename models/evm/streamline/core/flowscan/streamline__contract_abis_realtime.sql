{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = '{{this.schema}}.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table": "contract_abis",
        "sql_limit": "1000",
        "producer_batch_size": "250",
        "worker_batch_size": "250",
        "sql_source": "{{this.identifier}}" }
    )
) }}
with verified_contracts as (
    select address_hash AS contract_address from {{ ref('seeds__evm_verified_contracts') }}
    EXCEPT
    select contract_address from {{ ref('streamline__complete_contract_abis') }}
)
SELECT
    contract_address,
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        'https://evm.flowscan.io/api/v2/smart-contracts/' || contract_address,
        {},
        {}
    ) AS request
FROM
    verified_contracts
