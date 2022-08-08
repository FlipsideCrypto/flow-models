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
        _event_data_fields AS raw_attribute,
        COALESCE(
            raw_attribute [index] :staticType :typeID,
            raw_attribute [index] :value :value :value :value,
            raw_attribute [index] :value :value :value,
            raw_attribute [index] :value :value,
            raw_attribute [index] :value,
            raw_attribute
        ) AS attribute_value,
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
address_adjustment AS (
    SELECT
        attribute_id,
        LENGTH(attribute_value) AS ava_len,
        CONCAT(
            '0x',
            LPAD(SPLIT(attribute_value, '0x') [1], 16, '0') :: STRING
        ) AS address_adj
    FROM
        attributes
    WHERE
        attribute_value LIKE '0x%'
        AND ava_len < 19
),
FINAL AS (
    SELECT
        A.attribute_id,
        event_id,
        tx_id,
        block_timestamp,
        event_index,
        attribute_index,
        event_contract,
        event_type,
        raw_attribute,
        attribute_key,
        attribute_value,
        REPLACE(
            COALESCE(
                address_adj,
                attribute_value
            ),
            '"'
        ) AS attribute_value_adj,
        _ingested_at,
        _inserted_timestamp
    FROM
        attributes A
        LEFT JOIN address_adjustment USING (attribute_id)
)
SELECT
    *
FROM
    FINAL
