{{ config(
    materialized = 'view',
    tags = ['nft', 'ez']
) }}

WITH silver_nfts AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_sales') }}
    WHERE
        block_timestamp >= '2022-04-20'
),
gold_nfts AS (
    SELECT
        tx_id,
        block_height,
        block_timestamp,
        marketplace,
        nft_collection,
        nft_id,
        buyer,
        seller,
        price,
        currency,
        tx_succeeded,
        tokenflow,
        counterparties
    FROM
        silver_nfts
)
SELECT
    *
FROM
    gold_nfts
