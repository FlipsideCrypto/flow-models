{{ config(
  materialized = 'incremental',
  cluster_by = ['_ingested_at::DATE', 'block_timestamp::DATE'],
  unique_key = #TODO,
  incremental_strategy = 'delete+insert'
) }}

with 
events as (

    select * from {{ ref('silver__tx_events') }}

    {% if is_incremental() %}
        WHERE
            ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}

),

events_data as (
    select

        event_id,
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_index,
        event_contract,
        event_type,
        _event_data_type,
        _event_data_fields,
        _ingested_at,
        coalesce(event_data_type:fields, event_data_type:Fields) as event_data_type_fields

    from events
    
),

attributes as (

    select

        event_id,
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_index,
        event_contract,
        event_type,
        value:identifier as attribute_key,
        event_data_fields[index] as attribute_value,
        concat_ws('-', event_id, index) as attribute_id,
        _ingested_at

    from events_data, lateral flatten(input => event_data_type_fields)

),

final as (

    select
        attribute_id,
        event_id,
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_index,
        event_contract,
        event_type,
        attribute_key,
        attribute_value,
        _ingested_at

)

select * from final