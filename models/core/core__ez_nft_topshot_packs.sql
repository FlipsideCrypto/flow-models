{{ config(
    materialized = 'view',
    tags = ['nft', 'ez']
) }}

WITH silver_nfts AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_topshot_pack_sales') }}
),
gold_nfts AS (
    SELECT
        tx_id,
        block_height,
        block_timestamp,
        marketplace,
        nft_id,
        buyer,
        tx_succeeded
    FROM
        silver_nfts
)
SELECT
    *
FROM
    gold_nfts
