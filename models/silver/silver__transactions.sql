{{
  config(
    materialized='incremental',
    cluster_by='block_timestamp',
    unique_key='tx_id',
    incremental_strategy = 'delete+insert'
  )
}}

with
bronze_txs as (

  select * from {{ ref('bronze__txs') }}

  {% if is_incremental() %}
  where ingested_at::date >= getdate() - interval '2 days'
  {% endif %}

),

silver_txs as (

  select

      tx_id,
      block_timestamp,
      block_id,
      chain_id,
      case
        when tx:proposal_key:Address is null then tx:proposalKeyAddress::string
        else tx:proposal_key:Address::string
      end as proposer,
      tx:payer::string as payer,
      tx:authorizers::variant as authorizers,
      array_size(authorizers) as count_authorizers,
      case
        when tx:gas_limit is null then tx:gasLimit::number
        else tx:gas_limit::number
      end as gas_limit,
      case
        when tx:transaction_result is null then tx:result::variant
        else tx:transaction_result::variant
      end as transaction_result,
      case
        when transaction_result:error = '{}' then 'FAILED'
        else 'SUCCEEDED'
      end as error_status

  from bronze_txs

)

select * from silver_txs
