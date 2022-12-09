{{ config(
    materialized = 'table',
    unique_key = 'nft_id',
    cluster_by = ['nft_id'],
    sort = 'nft_id',
    tags = ['nft', 'dapper', 'topshot', 'nft-metadata']
) }}

WITH play_metadata AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_play_created') }}
    WHERE
        event_contract = 'A.0b2a3299cc857e29.TopShot'
),
nft_id_info AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_minted') }}
    WHERE
        event_contract = 'A.0b2a3299cc857e29.TopShot'
),
edition_info AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_topshot_subeditions') }}
),
FINAL AS (
    SELECT
        n.event_contract,
        n.nft_id,
        n.serial_number,
        n.set_id,
        n.play_id,
        e.subedition_id,
        e.edition_name,
        m.metadata
    FROM
        nft_id_info n
        LEFT JOIN play_metadata m USING (play_id)
        LEFT JOIN edition_info e USING (nft_id)
)
SELECT
    *
FROM
    FINAL
