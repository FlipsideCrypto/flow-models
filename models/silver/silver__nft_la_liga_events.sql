{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id',
    incremental_strategy = 'delete+insert'
) }}

WITH la_liga AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        event_contract ILIKE '%87ca73a41bb50ad5%'
        AND event_type IN (
            'PlayCreated',
            'EditionCreated',
            'SetCreated',
            'SeriesCreated'
        )
)
SELECT
    *
FROM
    la_liga
