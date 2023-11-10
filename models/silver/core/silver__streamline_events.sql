{{ config(
    materialized = 'incremental',
    unique_key = 'event_id',
    cluster_by = "_inserted_timestamp::date",
    tags = ['core', 'streamline_scheduled', 'scheduled', 'scheduled_core']
) }}

WITH transactions AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        NOT pending_result_response -- inserted timestamp will update w TR ingestion, so should flow thru to events and curated

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
flatten_events AS (
    SELECT
        block_height,
        block_timestamp,
        tx_id,
        tx_succeeded,
        VALUE :: variant AS event_data_full,
        VALUE :event_index :: INT AS event_index,
        concat_ws(
            '-',
            tx_id,
            event_index
        ) AS event_id,
        VALUE :payload :: STRING AS payload,
        TRY_PARSE_JSON(utils.udf_hex_to_string(payload)) AS decoded_payload,
        VALUE :type :: STRING AS event_type_id,
        VALUE :values :: variant AS event_values,
        COALESCE(
            SUBSTR(
                VALUE :type :: STRING,
                0,
                LENGTH(
                    VALUE :type :: STRING
                ) - LENGTH(SPLIT(VALUE :type :: STRING, '.') [3]) - 1
            ),
            -- if null, then flow.<event_type>
            SPLIT(
                VALUE :type :: STRING,
                '.'
            ) [0]
        ) AS event_contract,
        COALESCE(
            SPLIT(
                VALUE :type :: STRING,
                '.'
            ) [3],
            -- if null, then flow.<event_type>
            SPLIT(
                VALUE :type :: STRING,
                '.'
            ) [1]
        ) :: STRING AS event_type,
        _inserted_timestamp,
        _partition_by_block_id
    FROM
        transactions t,
        LATERAL FLATTEN(
            input => events
        ) e
),
attributes AS (
    SELECT
        event_id,
        OBJECT_AGG(
            VALUE :name :: variant,
            COALESCE(
                VALUE :value :value :fields,
                VALUE :value :value :staticType,
                VALUE :value :value :value :value :: STRING,
                VALUE :value :value :value :: STRING,
                VALUE :value :value :: STRING,
                'null'
            ) :: variant
        ) AS event_data
    FROM
        flatten_events,
        LATERAL FLATTEN (
            COALESCE(
                decoded_payload :value :fields :: variant,
                event_values :value :fields :: variant
            )
        )
    GROUP BY
        1
),
FINAL AS (
    SELECT
        e.tx_id,
        e.block_height,
        e.block_timestamp,
        e.event_id,
        e.event_index,
        e.payload,
        e.event_contract,
        e.event_type,
        A.event_data,
        e.tx_succeeded,
        e._inserted_timestamp,
        e._partition_by_block_id
    FROM
        flatten_events e
        LEFT JOIN attributes A USING (event_id)
)
SELECT
    *
FROM
    FINAL
