{{ config(
    materialized = 'view'
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
    WHERE
        tx_id != '8620792f30d607a35eb5a7ffe6ea2a088d448f1b706e8585ca8ae8697655e6fa'
)
SELECT
    *
FROM
    gold_nfts
