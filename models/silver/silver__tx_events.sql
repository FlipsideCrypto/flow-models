{{ config(
  materialized = 'incremental',
  cluster_by = ['ingested_at::DATE', 'block_timestamp::DATE'],
  unique_key = 'event_id',
  incremental_strategy = 'delete+insert'
) }}

WITH transactions AS (

  SELECT
    *
  FROM
    {{ ref('silver__transactions') }}

{% if is_incremental() %}
WHERE
  ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
events AS (
  SELECT
    tx_id,
    block_timestamp,
    block_height,
    tx_succeeded,
    COALESCE(
      VALUE :event_index,
      VALUE :eventIndex
    ) :: NUMBER AS event_index,
    SPLIT(
      VALUE :type,
      '.'
    ) AS type_split,
    CASE
      WHEN ARRAY_SIZE(type_split) = 4 THEN concat_ws(
        '.',
        type_split [0],
        type_split [1],
        type_split [2]
      ) :: STRING
      ELSE type_split [0] :: STRING
    END AS event_contract,
    CASE
      WHEN ARRAY_SIZE(type_split) = 4 THEN type_split [3] :: STRING
      ELSE type_split [1] :: STRING
    END AS event_type,
    VALUE :value :: variant AS event_data,
    COALESCE(
      VALUE :value :EventType,
      VALUE :value :eventType
    ) :: variant AS event_data_type,
    COALESCE(
      VALUE :value :Fields,
      VALUE :value :fields
    ) :: variant AS event_data_fields,
    concat_ws(
      '-',
      tx_id,
      event_index
    ) AS event_id,
    ingested_at
  FROM
    transactions,
    LATERAL FLATTEN(
      input => transaction_result :events
    )
),
FINAL AS (
  SELECT
    event_id,
    tx_id,
    block_timestamp,
    block_height,
    tx_succeeded,
    event_index,
    event_contract,
    event_type,
    event_data,
    event_data_type AS _event_data_type,
    event_data_fields AS _event_data_fields,
    ingested_at AS _ingested_at
  FROM
    events
)
SELECT
  *
FROM
  FINAL
