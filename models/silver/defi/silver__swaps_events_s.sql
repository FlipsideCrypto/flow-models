{{ config(
  materialized = 'incremental',
  cluster_by = ['_inserted_timestamp::DATE'],
  unique_key = "CONCAT_WS('-', tx_id, event_index)",
  incremental_strategy = 'delete+insert',
  tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH swaps_txs AS (

  SELECT
    block_height,
    tx_id,
    _inserted_timestamp,
    _partition_by_block_id,
    modified_timestamp
  FROM
    {{ ref('silver__streamline_events') }}
  WHERE
    event_contract LIKE '%SwapPair%'

{% if is_incremental() %}
AND modified_timestamp >= (
  SELECT
    MAX(modified_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
swap_events AS (
  SELECT
    tx_id,
    block_height,
    block_timestamp,
    event_id,
    event_index,
    events_count,
    payload,
    event_contract,
    event_type,
    event_data,
    tx_succeeded,
    _inserted_timestamp,
    _partition_by_block_id
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
AND modified_timestamp >= (
  SELECT
    MAX(modified_timestamp)
  FROM
    {{ this }}
)
{% endif %}
)
SELECT
  tx_id,
  block_height,
  block_timestamp,
  event_id,
  event_index,
  events_count,
  payload,
  event_contract,
  event_type,
  event_data,
  tx_succeeded,
  _inserted_timestamp,
  _partition_by_block_id,
  {{ dbt_utils.generate_surrogate_key(
      ['tx_id', 'event_index']
  ) }} AS swaps_events_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  swap_events
