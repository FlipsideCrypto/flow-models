{{ config(
  materialized = 'incremental',
  cluster_by = ['_inserted_timestamp::DATE'],
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
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  )
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
      IFF(
        VALUE :type = 'Event',
        VALUE :eventType :qualifiedIdentifier,
        VALUE :type
      ),
      '.'
    ) AS type_split,
    ARRAY_TO_STRING(
      ARRAY_SLICE(type_split, 0, ARRAY_SIZE(type_split) -1),
      '.') AS event_contract,
      type_split [array_size(type_split)-1] :: STRING AS event_type,
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
      _ingested_at,
      _inserted_timestamp
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
        _ingested_at,
        _inserted_timestamp
      FROM
        events
    )
  SELECT
    *
  FROM
    FINAL
