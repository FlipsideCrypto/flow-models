{{ config (
    materialized = 'view',
    tags = ['nft', 'dapper', 'scheduled'],
    meta = {
    'database_tags':{
        'table': {
            'PURPOSE': 'NFT, ALLDAY, GOLAZOS, TOPSHOT'
            }
        }
    }
) }}

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
    tier,
    metadata
FROM
    {{ ref('silver__nft_moment_metadata_final') }}
WHERE
    NOT (
        nft_collection = 'A.87ca73a41bb50ad5.Golazos'
        AND edition_id = 486
    )
    AND NOT (
        nft_collection = 'A.e4cf4bdc1751c65d.AllDay'
        AND edition_id = 1486
    )
