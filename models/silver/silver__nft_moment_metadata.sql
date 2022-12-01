{{ config(
    materialized = 'incremental',
    cluster_by = ['play_id'],
    unique_key = "concat_ws('-', event_contract, play_id)",
    incremental_strategy = 'delete+insert',
    tags = ['nft', 'dapper']
) }}

WITH play_creation AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        event_type = 'PlayCreated'
        
{# 
currently includes the following contracts
A.c38aea683c0c4d38.Eternal
A.b715b81853fef53f.AllDay
A.67af7ecf76556cd3.ABD
A.0b2a3299cc857e29.TopShot
A.5c0992b465832a94.TKNZ
A.e4cf4bdc1751c65d.AllDay
A.87ca73a41bb50ad5.Golazos
 #}


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
FINAL AS (
    SELECT
        tx_id,
        block_timestamp,
        event_contract,
        play_id,
        _inserted_timestamp,
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
        4,
        5
)
SELECT
    *
FROM
    FINAL
