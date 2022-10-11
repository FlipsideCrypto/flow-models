{{ config (
  materialized = 'view'
) }}

SELECT
  record_id,
  tx_id,
  tx_block_index,
  offset_id,
  block_id,
  block_timestamp,
  network,
  chain_id,
  tx,
  ingested_at AS _ingested_at,
  _inserted_timestamp
FROM
  {{ source(
    'prod',
    'flow_txs'
  ) }}
WHERE
  _inserted_timestamp :: DATE >= '2022-05-01'
