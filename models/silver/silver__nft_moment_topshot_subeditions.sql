{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp'],
    unique_key = "concat_ws('-', subedition_id, nft_id)",
    incremental_strategy = 'delete+insert',
    tags = ['nft', 'dapper', 'topshot', 'nft-metadata'],
) }}

WITH events AS (

    SELECT
        *
    FROM
        flow.silver.events_final
    WHERE
        event_contract = 'A.0b2a3299cc857e29.TopShot'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
subedition_created AS (
    SELECT
        tx_id,
        block_timestamp,
        _inserted_timestamp,
        event_contract,
        event_data :metadata :: variant AS edition_metadata,
        event_data :name :: STRING AS edition_name,
        event_data :subeditionID :: STRING AS subedition_id
    FROM
        events
    WHERE
        event_type = 'SubeditionCreated'
),
subedition_added AS (
    SELECT
        tx_id,
        block_timestamp,
        _inserted_timestamp,
        event_contract,
        event_data :momentID :: STRING AS nft_id,
        event_data :playID :: STRING AS play_id,
        event_data :setID :: STRING AS set_id,
        event_data :subeditionID :: STRING AS subedition_id
    FROM
        events
    WHERE
        event_type = 'SubeditionAddedToMoment'
),
FINAL AS (
    SELECT
        sc.subedition_id,
        sc.edition_name,
        sc.edition_metadata,
        sa.nft_id,
        sa.play_id,
        sa.set_id,
        sa._inserted_timestamp
    FROM
        subedition_added sa
        LEFT JOIN subedition_created sc USING (subedition_id)
)
SELECT
    *
FROM
    FINAL
