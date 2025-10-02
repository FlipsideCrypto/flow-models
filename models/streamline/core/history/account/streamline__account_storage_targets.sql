{{ config (
    materialized = "table",
    tags = ['streamline_history', 'account_storage']
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
to_do as (
    select 
        block_date,
        block_height,
        account_address,
        event_contract
    from account_addresses
    -- add exclude here 
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
    from to_do 
)
select * from create_requests