{{
  config(
    materialized='incremental',
    cluster_by=['ingested_at::DATA', 'block_timestamp::DATA'],
    unique_key='event_id',
    incremental_strategy = 'delete+insert'
  )
}}

with
transactions as (

  select * from {{ ref('silver__transactions') }}

),

events as (

  select

      tx_id,
      block_timestamp,
      block_id,
      error_status,
      coalesce(value:event_index, value:eventIndex)::number as event_index,
      split(value:type, '.') as type_split,
      case
        when array_size(type_split) = 4 then concat_ws('.', type_split[0], type_split[1], type_split[2])::string
        else type_split[0]::string
      end as event_contract,
      case
        when array_size(type_split) = 4 then type_split[3]::string
        else type_split[1]::string
      end as event_type,
      value:value::variant as event_data,
      coalesce(value:value:EventType, value:value:eventType)::variant as event_data_type,
      coalesce(value:value:Fields, value:value:fields)::variant as event_data_fields,
      concat_ws('-', tx_id, event_index) as event_id,
      ingested_at

  from transactions, lateral flatten(input => transaction_result:events)

),

final as (

  select

    event_id,
    tx_id,
    block_timestamp,
    block_id,
    error_status,
    event_index,
    event_contract,
    event_type,
    event_data,
    event_data_type,
    event_data_fields,
    ingested_at

  from events

)

select * from final
