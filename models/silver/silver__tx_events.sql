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

  select * from {{ ref('silver__transactions') }}

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
      value:type::string as type,
      value:value::variant as event_value,
      case
          when value:value:EventType is null then value:value:eventType::variant
          else value:value:EventType::variant
      end as event_type,
      case
          when value:value:Fields is null then value:value:fields::variant
          else value:value:Fields::variant
      end as event_fields,
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
    type,
    event_value,
    event_type,
    event_fields

  from events

)

select * from final
