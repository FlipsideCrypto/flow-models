{{ config(
    materialized = 'view',
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'NFT, UFCSTRIKE'
            }
        }
    },
    tags = ['scheduled_non_core']
) }}

SELECT
    nft_id,
    serial_number,
    listing_id,
    set_name,
    metadata,
    inserted_timestamp,
    modified_timestamp,
    invocation_id
FROM
    {{ ref('silver__nft_ufc_strike_metadata') }}
