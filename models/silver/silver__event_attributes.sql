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
        COALESCE(
            _event_data_fields [index] :Value,
            _event_data_fields [index]
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
handle_address_arrays AS (
    SELECT
        attribute_id,
        b.index,
        LPAD(TRIM(to_char(b.value :: INT, 'XXXXXXX')) :: STRING, 2, '0') AS hex
    FROM
        attributes A,
        TABLE(FLATTEN(attribute_value, recursive => TRUE)) b
    WHERE
        IS_ARRAY(attribute_value) = TRUE
    ORDER BY
        1,
        2
),
recombine_address AS (
    SELECT
        attribute_id,
        CONCAT(
            '0x',
            ARRAY_TO_STRING(ARRAY_AGG(hex) within GROUP (
            ORDER BY
                INDEX ASC), '')
        ) AS decoded_address
    FROM
        handle_address_arrays
    GROUP BY
        1
),
replace_arrays AS (
    SELECT
        A.attribute_id,
        event_id,
        tx_id,
        block_timestamp,
        event_index,
        attribute_index,
        event_contract,
        event_type,
        attribute_key,
        attribute_value,
        decoded_address,
        COALESCE(
            decoded_address,
            attribute_value
        ) :: STRING AS attribute_value_adj,
        _ingested_at,
        _inserted_timestamp
    FROM
        attributes A
        LEFT JOIN recombine_address USING (attribute_id)
),
address_adjustment AS (
    SELECT
        attribute_id,
        LENGTH(attribute_value_adj) AS ava_len,
        CONCAT(
            '0x',
            LPAD(SPLIT(attribute_value_adj, '0x') [1], 16, '0') :: STRING
        ) AS address_adj
    FROM
        replace_arrays
    WHERE
        attribute_value_adj LIKE '0x%'
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
        attribute_key,
        decoded_address,
        attribute_value,
        REPLACE(
            COALESCE(
                address_adj,
                attribute_value_adj
            ),
            '"'
        ) AS attribute_value_adj,
        _ingested_at,
        _inserted_timestamp
    FROM
        replace_arrays A
        LEFT JOIN address_adjustment USING (attribute_id)
)
SELECT
    *
FROM
    FINAL
