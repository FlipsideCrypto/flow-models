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
        AND event_type = 'MomentNFTMinted'

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
        event_data :editionID :: STRING AS edition_id,
        event_data :id :: STRING AS nft_id,
        event_data :serialNumber :: STRING AS serial_number,
        _inserted_timestamp
    FROM
        events
)
SELECT
    *
FROM
    org
