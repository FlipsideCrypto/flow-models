{{ config(
    materialized = 'incremental',
    cluster_by = ['play_id'],
    unique_key = "concat_ws('-', collection, play_id)",
    incremental_strategy = 'delete+insert'
) }}

WITH play_creation AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        event_type = 'PlayCreated'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
play_metadata AS (
    SELECT
        tx_id,
        block_timestamp,
        event_contract,
        event_data :id :: NUMBER AS play_id,
        VALUE :key :value :: STRING AS column_header,
        VALUE :value :value :: STRING AS column_value,
        _inserted_timestamp
    FROM
        play_creation,
        LATERAL FLATTEN(input => TRY_PARSE_JSON(event_data :metadata))
),
neat_object AS (
    SELECT
        tx_id,
        block_timestamp,
        event_contract,
        play_id,
        OBJECT_AGG(
            column_header :: variant,
            column_value :: variant
        ) AS metadata
    FROM
        play_metadata
    GROUP BY
        1,
        2,
        3,
        4
)
SELECT
    *
FROM
    neat_object
