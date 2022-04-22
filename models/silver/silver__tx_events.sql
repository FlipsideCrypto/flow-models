{{
  config(
    materialized='incremental',
    cluster_by='block_timestamp',
    unique_key='event_id',
    incremental_strategy = 'delete+insert',
    tags=['silver','transactions', 'events']
  )
}}

with
transactions as (

  select * from flow_dev.silver.transactions

),

events as (

  select

      tx_id,
      block_timestamp,
      block_id,
      error_status,
      case
          when value:event_index is null then value:eventIndex::number
          else value:event_index::number
      end as event_index,
      split(value:type, '.') as type_split,
      case
        when array_size(type_split) = 4 then concat_ws('.', type_split[0], type_split[1], type_split[2])::string
        else type_split[0]::string
      end as contract,
      case
        when array_size(type_split) = 4 then type_split[3]::string
        else type_split[1]::string
      end as type,
      value:value::variant as event_data,
      case
          when value:value:EventType is null then value:value:eventType::variant
          else value:value:EventType::variant
      end as event_data_type,
      case
          when value:value:Fields is null then value:value:fields::variant
          else value:value:Fields::variant
      end as event_data_fields,
      concat_ws('-', tx_id, event_index) as event_id

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
    contract,
    type,
    event_data,
    event_data_type,
    event_data_fields

  from events

)

select * from final
