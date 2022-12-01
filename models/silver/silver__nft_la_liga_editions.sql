{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp'],
    unique_key = "concat_ws('-', event_contract, edition_id)",
    incremental_strategy = 'delete+insert'
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        event_contract ILIKE '%87ca73a41bb50ad5%'
        AND event_type = 'EditionCreated'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
org AS (
    SELECT
        tx_id,
        block_timestamp,
        event_contract,
        event_data :id :: STRING AS edition_id,
        event_data :maxMintSize :: STRING AS max_mint_size,
        event_data :playID :: STRING AS play_id,
        event_data :seriesID :: STRING AS series_id,
        event_data :setID :: STRING AS set_id,
        event_data :tier :: STRING AS tier,
        _inserted_timestamp,
        'abc' as new,
        event_data
    FROM
        events
)
SELECT
    *
FROM
    org
