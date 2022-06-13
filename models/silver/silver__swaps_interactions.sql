{{ config(
  materialized = 'incremental',
  cluster_by = ['_ingested_at::DATE', 'block_timestamp::DATE'],
  unique_key = 'block_height',
  incremental_strategy = 'delete+insert'
) }}

WITH swap_contracts AS (

  SELECT
    *
  FROM
    {{ ref('silver__contract_labels') }}
  WHERE
    contract_name LIKE '%SwapPair%'
),
swaps_txs AS (
  SELECT
    *
  FROM
    {{ ref('silver__events_final') }}
  WHERE
    event_contract IN (
      SELECT
        event_contract
      FROM
        swap_contracts
    )
),
swap_events AS (
  SELECT
    *
  FROM
    {{ ref('silver__events_final') }}
  WHERE
    tx_id IN (
      SELECT
        tx_id
      FROM
        swaps_txs
    )
)
SELECT
  *
FROM
  swap_events
