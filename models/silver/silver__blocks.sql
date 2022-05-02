{{
  config(
    materialized='incremental',
    cluster_by='block_timestamp',
    unique_key='block_height',
    incremental_strategy = 'delete+insert',
    tags=['silver','blocks']
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
      case
          when (header:block_id is null and header:block_header:block_id is null) then header:id::string
          when (header:block_id is null and header:id is null) then header:block_header:block_id::string
          else header:block_id::string
      end as id,
      case
          when (header:parent_id is null and header:parentId is null) then header:block_header:parent_id::string
          when (header:parent_id is null and header:block_header:parent_id is null) then header:parentId::string
          else header:parent_id::string
      end as parent_id,
      case
          when header:block_header:collection_guarantee is null then header:collection_guarantee::variant
          else header:block_header:collection_guarantee::variant
      end as collection_guarantee,
      ingested_at

  from bronze_blocks

)

select * from silver_blocks
