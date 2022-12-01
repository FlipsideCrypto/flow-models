{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp'],
    unique_key = "concat_ws('-', event_contract, moment_id)",
    incremental_strategy = 'delete+insert',
    tags = ['nft', 'dapper']
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        event_type = 'MomentMinted'

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
        event_data :contentEditionID :: STRING AS content_edition_id,
        event_data :contentID :: STRING AS content_id,
        event_data :momentID :: STRING AS moment_id,
        event_data :serialNumber :: STRING AS serial_number,
        event_data :seriesID :: STRING AS series_id,
        event_data :setID :: STRING AS set_id,
        _inserted_timestamp
    FROM
        events
)
SELECT
    *
FROM
    org
