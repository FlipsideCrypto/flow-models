{{ config(
    materialized = 'incremental',
    unique_key = 'event_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = "block_timestamp::date",
    tags = ['testnet']
) }}

WITH transactions AS (

    SELECT
        *
    FROM
        {{ ref('silver__testnet_transactions_final') }}
    WHERE
        NOT pending_result_response
        AND block_height >= 211000000 -- Aug 14 2024, Crescendo Upgrade

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
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
        events_count,
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
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _inserted_timestamp DESC) = 1

),
attributes AS (
    SELECT
        event_id,
        OBJECT_AGG(
            data_key,
            IFF(IS_ARRAY(TRY_PARSE_JSON(data_value)) OR  IS_OBJECT(TRY_PARSE_JSON(data_value)), PARSE_JSON(data_value)::VARIANT, data_value::VARIANT)
            ) AS event_data
            FROM
                (
                    SELECT
                        event_id,
                        VALUE :name :: variant AS data_key,
                        COALESCE(
                            VALUE :value :value :fields,
                            VALUE :value :value :staticType,
                            VALUE :value :value :value :value :: STRING,
                            VALUE :value :value :value :: STRING,
                            VALUE :value :value :: STRING,
                            'null'
                        ) AS data_value
                    FROM
                        flatten_events,
                        LATERAL FLATTEN (
                            COALESCE(
                                decoded_payload :value :fields :: variant,
                                event_values :value :fields :: variant
                            )
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
                e.events_count,
                e.payload,
                e.event_contract,
                e.event_type,
                A.event_data,
                e.tx_succeeded,
                e._inserted_timestamp,
                e._partition_by_block_id,
                {{ dbt_utils.generate_surrogate_key(
                    ['event_id']
                ) }} AS streamline_event_id,
                SYSDATE() AS inserted_timestamp,
                SYSDATE() AS modified_timestamp,
                '{{ invocation_id }}' AS _invocation_id
            FROM
                flatten_events e
                LEFT JOIN attributes A USING (event_id)
        )
    SELECT
        *
    FROM
        FINAL
