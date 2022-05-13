{{ config(
  materialized = 'incremental',
  cluster_by = ['block_timestamp::DATE'],
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert'
) }}

with
silver_txs as (

    select * from {{ ref('silver__transactions') }}
    {% if is_incremental() %}
    where _ingested_at::date > CURRENT_DATE - 2
    {% endif %}

),

gold_txs as (

    select
        tx_id,
        block_timestamp,
        block_height,
        chain_id,
        tx_index,
        proposer,
        payer,
        authorizers,
        count_authorizers,
        gas_limit,
        transaction_result,
        tx_succeeded,
        error_msg
    from silver_txs
)

select * from gold_txs