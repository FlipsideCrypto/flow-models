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
        attribute_key,
        attribute_value_adj,
        object_construct(attribute_key, attribute_value_adj) as kvp
    from silver_event_attributes

),

array_build as (
    select
        event_id,
        array_agg(kvp) as event_data
    from objs
    group by 1
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
    from silver_events e
        left join array_build a using (event_id)
)

select * from gold_events