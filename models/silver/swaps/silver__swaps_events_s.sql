{{ config(
  materialized = 'incremental',
  cluster_by = ['_inserted_timestamp::DATE'],
  unique_key = "CONCAT_WS('-', tx_id, event_index)",
  incremental_strategy = 'delete+insert',
  tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH swaps_txs AS (

  SELECT
    *
  FROM
    {{ ref('silver__streamline_events') }}
  WHERE
    event_contract LIKE '%SwapPair%'

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
    {{ ref('silver__streamline_events') }}
  WHERE
    tx_id IN (
      SELECT
        tx_id
      FROM
        swaps_txs
    ) 
    -- exclude infra events, always final 3
    AND event_index < events_count - 3

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
