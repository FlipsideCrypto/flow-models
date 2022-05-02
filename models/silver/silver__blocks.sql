{{
  config(
    materialized='incremental',
    cluster_by=['ingested_at::DATE', 'block_timestamp::DATE'],
    unique_key='block_height',
    incremental_strategy = 'delete+insert'
  )
}}

with
bronze_blocks as (

  select * from {{ ref('bronze__blocks') }}

  {% if is_incremental() %}
  where ingested_at::date >= getdate() - interval '2 days'
  {% endif %}

  qualify row_number() over (partition by block_id order by ingested_at desc) = 1

),

silver_blocks as (

  select

      block_id as block_height,
      block_timestamp,
      network,
      chain_id,
      tx_count,
      coalesce(header:block_id, header:block_header:block_id, header:id)::string as id,
      coalesce(header:parent_id, header:parentId, header:block_header:parent_id)::string as parent_id,
      coalesce(header:block_header:collection_guarantee, header:collection_guarantee)::variant as collection_guarantee,
      ingested_at

  from bronze_blocks

)

select * from silver_blocks
