{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        cluster_by=['block_timestamp::date'],
        unique_key='event_id'
    )
}}

with silver_events as (

    select * from {{ ref('silver__events') }}

    {% if is_incremental() %}
    where
        _ingested_at::date >= current_date -2 
    {% endif %}

),

silver_event_attributes as (

    select * from {{ ref('silver__event_attributes') }}
{% if is_incremental() %}
    where
        _ingested_at::date >= current_date -2 
    {% endif %}

),

objs as (

    select
        event_id,
        object_agg(attribute_key, attribute_value_adj::variant) as event_data
    from silver_event_attributes
    group by 1

),

location_object as (

    select
        event_id,
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_index,
        event_contract,
        event_type,
        coalesce(_event_data_type:location, _event_data_type:Location) as event_data
    from silver_events
    where _event_data_fields = '[]'
),


gold_events as (

    select
        e.event_id,
        e.tx_id,
        e.block_timestamp,
        e.block_height,
        e.tx_succeeded,
        e.event_index,
        e.event_contract,
        e.event_type,
        a.event_data
    from objs a
        left join silver_events e using (event_id)
),

final as (

    select * from gold_events
    union
    select * from location_object
)

select * from final