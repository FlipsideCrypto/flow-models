{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"account_storage",
        "sql_limit" :"100000",
        "producer_batch_size" :"50000",
        "worker_batch_size" :"10000",
        "async_concurrent_requests" :"10",
        "sql_source" :"{{this.identifier}}" }
    ),
    tags = ['streamline_realtime', 'account_storage']
) }}

with all_event_contracts as (
    select 
        min(block_height) as events_start,
        event_contract
    from {{ ref('core__fact_events') }}
    where tx_succeeded
    group by all 
),
relevant_block_heights as (
    select 
        block_timestamp::date as block_date,
        max(block_height) as block_height
    from {{ ref('core__fact_blocks') }}
    where block_timestamp >= '2025-01-01'
    and block_timestamp::date <> (select max(block_timestamp::date) from {{ ref('core__fact_blocks') }})
    and block_timestamp::date >= DATEADD('day', -4, SYSDATE())
    group by all 
),
event_contract_days as (
    select
        event_contract,
        block_date,
        block_height
    from all_event_contracts a 
    join relevant_block_heights b 
    on a.events_start <= b.block_height
),
account_addresses as (
    select 
        event_contract,
        block_date,
        block_height,
        account_address
    from event_contract_days
    join {{ ref('core__dim_contract_labels') }} 
    using (event_contract)
),
distinct_targets as (
    select 
        block_date,
        block_height,
        account_address,
        event_contract
    from account_addresses
    qualify(ROW_NUMBER() over (PARTITION BY block_height, account_address ORDER BY block_date DESC)) = 1
),
create_requests as (
    select 
        block_date,
        block_height,
        account_address,
        event_contract,
        concat('/v1/scripts?block_height=', block_height) as endpoint,
            OBJECT_CONSTRUCT(
                'script', BASE64_ENCODE('access(all) fun main(addr: Address): [UInt64] { let account = getAccount(addr); return [account.storage.used, account.storage.capacity] }'),
                'arguments', ARRAY_CONSTRUCT(
                    BASE64_ENCODE('{"type":"Address","value":"' || account_address || '"}')
                )
            ) as request_data
    from distinct_targets 
),
to_do as (
    select 
        block_date,
        block_height,
        account_address,
        event_contract,
        endpoint,
        request_data
    from create_requests a 
    left join {{ ref('streamline__complete_get_account_storage') }} b 
    using (block_height, account_address)
    where b.complete_account_storage_id is null
)

select
    block_date::VARCHAR as block_date,
    round(block_height, -3) :: INT AS partition_key,
    block_height,
    account_address,
    event_contract,
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}/{Authentication}' || endpoint,
        OBJECT_CONSTRUCT(
            'Content-Type', 'application/json',
            'Accept', 'application/json'
        ),
        request_data,
        'Vault/prod/flow/quicknode/mainnet'
    ) as request
from to_do
order by block_date desc
limit 100000