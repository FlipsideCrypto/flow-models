{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"account_storage",
        "sql_limit" :"2000000",
        "producer_batch_size" :"100000",
        "worker_batch_size" :"10000",
        "async_concurrent_requests" :"10",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['streamline_history', 'account_storage']
) }}

with to_do as (
    select 
        block_date,
        block_height,
        account_address,
        event_contract,
        endpoint,
        request_data
    from {{ ref('streamline__account_storage_targets') }}
    order by block_date desc
    limit 5
)

select 
    block_date,
    round(block_height, -3) :: INT AS partition_key,
    block_height,
    account_address,
    event_contract,
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}/{Authentication}' || endpoint,
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'Accept', 'application/json',
            'fsc-quantum-state', 'livequery'
        ),
        request_data,
        'Vault/prod/flow/quicknode/mainnet'
    ) as request
from to_do