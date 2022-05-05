{{ config(
  materialized = 'incremental',
  cluster_by = ['_ingested_at::DATE', 'block_timestamp::DATE'],
  unique_key = 'attribute_id',
  incremental_strategy = 'delete+insert'
) }}

with 
events as (

    select * from {{ ref('silver__events') }}

    {% if is_incremental() %}
        WHERE
            _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}

),

events_data as (
    select

        event_id,
        tx_id,
        block_timestamp,
        event_index,
        event_contract,
        event_type,
        _event_data_type,
        _event_data_fields,
        _ingested_at,
        coalesce(_event_data_type:fields, _event_data_type:Fields) as event_data_type_fields

    from events
    
),

attributes as (

    select

        event_id,
        tx_id,
        block_timestamp,
        event_index,
        event_contract,
        event_type,
        coalesce(value:identifier, value:Identifier)::string as attribute_key,
        coalesce(_event_data_fields[index]:Value, _event_data_fields[index]) as attribute_value,
        concat_ws('-', event_id, index) as attribute_id,
        _ingested_at

    from events_data, lateral flatten(input => event_data_type_fields)

),

handle_address_arrays as (

    select

        attribute_id,
        b.index,
        lpad(trim(to_char(b.value::int,'XXXXXXX'))::string,2,'0') as hex

    from attributes a, table(flatten(attribute_value, recursive=>true)) b

    where is_array(attribute_value) = true
    order by 1,2

),

recombine_address as (

    select

        attribute_id,
        concat('0x',array_to_string(array_agg(hex) within group (order by index asc),'')) as decoded_address

    from handle_address_arrays
    group by 1

),

replace_arrays as (

    select

        a.attribute_id,
        event_id,
        tx_id,
        block_timestamp,
        event_index,
        event_contract,
        event_type,
        attribute_key,
        attribute_value,
        decoded_address,
        coalesce(decoded_address, attribute_value)::string as attribute_value_adj,
        _ingested_at

    from attributes a
        left join recombine_address using (attribute_id)

),

address_adjustment as (

    select 

        attribute_id,
        length(attribute_value_adj) as ava_len, 
        concat('0x',lpad(split(attribute_value_adj, '0x')[1],16,'0')::string) as address_adj

    from replace_arrays
    where attribute_value_adj like '0x%'
        and ava_len < 19


),

final as (

    select
    
        a.attribute_id,
        event_id,
        tx_id,
        block_timestamp,
        event_index,
        event_contract,
        event_type,
        attribute_key,
        decoded_address,
        attribute_value,
        coalesce(address_adj, attribute_value_adj) as attribute_value_adj,
        _ingested_at

    from replace_arrays a
    left join address_adjustment using (attribute_id)

)

select * from final