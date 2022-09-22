{{ config(
  materialized = 'incremental',
  cluster_by = ['_inserted_timestamp::DATE'],
  unique_key = "CONCAT_WS('-', tx_id, event_index)",
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
    event_data :: STRING != '{}'
    AND event_contract IN (
      SELECT
        event_contract
      FROM
        swap_contracts
    )

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
swap_events AS (
  SELECT
    *
  FROM
    {{ ref('silver__events_final') }}
  WHERE
    event_data :: STRING != '{}'
    AND tx_id IN (
      SELECT
        tx_id
      FROM
        swaps_txs
    )

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
)
SELECT
  *
FROM
  swap_events
