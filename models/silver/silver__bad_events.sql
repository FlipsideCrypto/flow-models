{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::date'],
    unique_key = "CONCAT_WS('-', tx_id, event_index)"
) }}

{# 
    Prior bad events check
 #}

WITH event_key AS (

    SELECT
        tx_id,
        block_height,
        block_timestamp,
        _inserted_timestamp,
        event_index,
        'n/a' AS _index_from_flatten,
        'null_attr_key' AS problem
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        event_data :: STRING = '{}'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
isolated_event AS (
    SELECT
        tx_id,
        block_height,
        block_timestamp,
        _inserted_timestamp,
        event_index,
        _index_from_flatten,
        'cadence' AS problem
    FROM
        {{ ref('silver__events') }}
    WHERE
        event_index IS NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
missing_event_data AS (
    SELECT
        tx_id,
        block_height,
        block_timestamp,
        _inserted_timestamp,
        event_index,
        _index_from_flatten,
        'no_events' AS problem
    FROM
        {{ ref('silver__events') }}
    WHERE
        _try_parse_payload IS NULL
        AND _attribute_fields IS NULL
        AND tx_id NOT IN (
            SELECT
                tx_id
            FROM
                isolated_event
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
all_bad AS (
    SELECT
        *
    FROM
        event_key
    UNION
    SELECT
        *
    FROM
        isolated_event
    UNION
    SELECT
        *
    FROM
        missing_event_data
),
FINAL AS (
    SELECT
        tx_id,
        block_height,
        block_timestamp,
        _inserted_timestamp,
        event_index,
        _index_from_flatten,
        problem,
        CURRENT_TIMESTAMP as _bad_event_record_date
    FROM
        all_bad
)
SELECT
    *
FROM
    FINAL
