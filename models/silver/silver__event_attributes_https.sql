{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'attribute_id',
    incremental_strategy = 'delete+insert'
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events') }}
    WHERE
        _inserted_timestamp :: DATE >= '2022-07-18'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
events_data AS (
    SELECT
        event_id,
        tx_id,
        block_timestamp,
        event_index,
        event_contract,
        event_type,
        _event_data_type,
        _event_data_fields,
        _ingested_at,
        COALESCE(
            _event_data_type :fields,
            _event_data_type :Fields
        ) AS event_data_type_fields,
        _inserted_timestamp
    FROM
        events
),
attributes AS (
    SELECT
        event_id,
        tx_id,
        block_timestamp,
        event_index,
        event_contract,
        event_type,
        COALESCE(
            VALUE :identifier,
            VALUE :Identifier
        ) :: STRING AS attribute_key,
        COALESCE(
            _event_data_fields [index] :fields,
            _event_data_fields [index] :staticType :typeID,
            _event_data_fields [index] :value :value,
            _event_data_fields [index] :value,
            _event_data_fields [index] :Value,
            _event_data_fields [index]
        ) :: STRING AS attribute_value,
        concat_ws(
            '-',
            event_id,
            INDEX
        ) AS attribute_id,
        INDEX AS attribute_index,
        _ingested_at,
        _inserted_timestamp
    FROM
        events_data,
        LATERAL FLATTEN(
            input => event_data_type_fields
        )
),
FINAL AS (
    SELECT
        attribute_id,
        event_id,
        tx_id,
        block_timestamp,
        event_index,
        attribute_index,
        event_contract,
        event_type,
        attribute_key,
        attribute_value,
        _ingested_at,
        _inserted_timestamp
    FROM
        attributes
    WHERE
        attribute_key IS NOT NULL
)
SELECT
    *
FROM
    FINAL
