{{ config (
    materialized = 'view',
    tags = ['nft', 'dapper', 'nft-metadata']
) }}

WITH topshot AS (

    SELECT
        event_contract AS nft_collection,
        nft_id,
        serial_number,
        NULL AS max_mint_size,
        play_id,
        NULL AS series_id,
        NULL AS series_name,
        set_id,
        NULL AS set_name,
        subedition_id AS edition_id,
        edition_name,
        NULL AS tier,
        metadata
    FROM
        {{ ref('silver__nft_moment_topshot_metadata') }}
    WHERE
        metadata IS NOT NULL
),
moments AS (
    SELECT
        event_contract AS nft_collection,
        nft_id,
        serial_number,
        max_mint_size,
        play_id,
        series_id,
        series_name,
        set_id,
        set_name,
        edition_id,
        NULL AS edition_name,
        tier,
        metadata
    FROM
        {{ ref('silver__nft_moment_metadata_final') }}
),
FINAL AS (
    SELECT
        *
    FROM
        topshot
    UNION
    SELECT
        *
    FROM
        moments
)
SELECT
    *
FROM
    FINAL
