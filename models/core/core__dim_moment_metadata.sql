{{ config (
    materialized = 'view',
    tags = ['nft', 'dapper']
) }}

SELECT
    event_contract as nft_collection,
    nft_id,
    serial_number,
    max_mint_size,
    play_id,
    series_id,
    series_name,
    set_id,
    set_name,
    edition_id,
    tier,
    metadata
FROM
    {{ ref('silver__nft_moment_metadata_final') }}
