{{ config(
  materialized = 'incremental',
  cluster_by = ['_ingested_at::DATE', 'block_timestamp::DATE'],
  unique_key = 'tx_id',
  incremental_strategy = 'delete+insert'
) }}

WITH bronze_txs AS (

  SELECT
    *
  FROM
    {{ ref('bronze__transactions') }}

{% if is_incremental() %}
WHERE
  _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}

qualify ROW_NUMBER() over (
  PARTITION BY tx_id
  ORDER BY
    _ingested_at DESC
) = 1
),
FINAL AS (
  SELECT
    tx_id,
    block_timestamp,
    block_id AS block_height,
    chain_id,
    tx_block_index,
    COALESCE(
      tx :proposal_key :Address,
      tx :proposalKeyAddress
    ) :: STRING AS proposer,
    tx :payer :: STRING AS payer,
    tx :authorizers :: ARRAY AS authorizers,
    ARRAY_SIZE(authorizers) AS count_authorizers,
    COALESCE(
      tx :gas_limit,
      tx :gasLimit
    ) :: NUMBER AS gas_limit,
    COALESCE(
      tx :transaction_result,
      tx :result
    ) :: variant AS transaction_result,
    CASE
      WHEN transaction_result :error = '' THEN TRUE
      WHEN TYPEOF(
        transaction_result :error
      ) = 'NULL_VALUE' THEN TRUE
      ELSE FALSE
    END AS tx_succeeded,
    COALESCE(
      transaction_result :error,
      ''
    ) :: STRING AS error_msg,
    _ingested_at
  FROM
    bronze_txs
)
SELECT
  *
FROM
  FINAL
