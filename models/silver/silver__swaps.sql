{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = "CONCAT_WS('-', tx_id, swap_index)",
    incremental_strategy = 'delete+insert'
) }}


with
swap_events as (
select * from {{ref('silver__swaps_events')}}
),
pool_info as (
select
    tx_id,
    block_timestamp,
    block_height,
    event_index,
    event_type,
    rank() over (partition by tx_id order by event_index) - 1 as swap_index,
    event_contract as pool_contract,
    case
        when lower(object_keys(event_data)[0]::string) = 'side' then 'Blocto'
        when lower(object_keys(event_data)[0]::string) = 'direction' then 'Increment'
        else 'Other'
    end as likely_dex,
    coalesce(event_data:direction::number, event_data:side::number - 1) as direction,
    coalesce(event_data:inTokenAmount, event_data:token1Amount)::double as in_token_amount,
    coalesce(event_data:outTokenAmount, event_data:token2Amount)::double as out_token_amount,
    _inserted_timestamp
from swap_events
    where event_type in ('Trade', 'Swap')
)
,
token_withdraws as (
select
    tx_id,
    block_timestamp,
    block_height,
    event_index,
    rank() over (partition by tx_id order by event_index) - 1 as token_index,
    event_contract,
    event_data,
    _inserted_timestamp
from swap_events
    where event_type = 'TokensWithdrawn'
        and tx_id in (select distinct tx_id from pool_info)
        and tx_id not in (select distinct tx_id from swap_events where event_type = 'RewardTokensWithdrawn')
),
token_deposits as (
select
    tx_id,
    block_timestamp,
    block_height,
    event_index,
    rank() over (partition by tx_id order by event_index) - 1 as token_index,
    event_contract,
    event_data,
    _inserted_timestamp
from swap_events
    where event_type = 'TokensDeposited'
        and tx_id in (select distinct tx_id from pool_info)
        and tx_id not in (select distinct tx_id from swap_events where event_type = 'RewardTokensWithdrawn')
),
link_token_movement as (
select
    w.tx_id,
    w.block_timestamp,
    w.block_height,
    w._inserted_timestamp,
    w.token_index,
    w.event_index as event_index_w,
    d.event_index as event_index_d,
    token_index as transfer_index,
    w.event_data:from::string as withdraw_from,
    d.event_data:to::string as deposit_to,
    w.event_data:amount::double as amount,
    w.event_contract as token_contract,
    w.token_index = d.token_index as token_check,
    w.event_contract = d.event_contract as contract_check,
    w.event_data:amount::double = d.event_data:amount::double as amount_check
from token_withdraws w 
    left join token_deposits d using (tx_id, token_index, event_contract)
)

,
restructure as (
select
    t.tx_id,
    t.transfer_index,
    p.swap_index,
    rank() over (partition by t.tx_id, swap_index order by transfer_index) - 1 as token_position,
    t.withdraw_from,
    t.deposit_to,
    t.amount,
    t.token_contract,
    p.pool_contract,
    p.direction,
    p.in_token_amount,
    p.out_token_amount
from link_token_movement t
    left join pool_info p on p.tx_id = t.tx_id 
                                and (p.in_token_amount = t.amount 
                                    or round(p.in_token_amount / 0.997,3) = round(t.amount,3) -- blocto takes a 0.3% fee out of the initial inToken
                                    or p.out_token_amount = t.amount
                                    or round(p.out_token_amount / 0.997,3) = round(t.amount,3) -- blocto takes a 0.3% fee out of the initial outToken
                                    )
where swap_index is not null -- exclude the network fee token movement
)

,

pool_token_alignment as (
select
    tx_id,
    pool_contract,
    swap_index,
    object_agg(concat('token',token_position), token_contract::variant) as tokens,
    object_agg(concat('amount',token_position), amount) as amounts
from restructure
group by 1,2,3
)
,
boilerplate as (
select
    tx_id,
    block_timestamp,
    block_height,
    _inserted_timestamp,
    withdraw_from as trader
from link_token_movement
    where transfer_index = 0
),
final as (
select
    tx_id,
    block_timestamp,
    block_height,
    pool_contract as swap_contract,
    swap_index,
    trader,
    tokens:token0::string as token_out_contract,
    amounts:amount0::double as token_out_amount,
    tokens:token1::string as token_in_contract,
    amounts:amount1::double as token_in_amount,
    _inserted_timestamp
from boilerplate
left join pool_token_alignment using (tx_id)
)
select * from final