{{ config(
    materialized = 'view',
    tags = ['nft', 'ez', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} }
) }}

WITH chainwalkers AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_sales') }}
    WHERE
        block_height < {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
streamline AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_sales_s') }}
    WHERE
        block_height >= {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
FINAL AS (
    SELECT
        NULL AS nft_sales_id,
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
        counterparties,
        NULL AS inserted_timestamp,
        _inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        chainwalkers
    UNION ALL
    SELECT
        nft_sales_id,
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
        counterparties,
        inserted_timestamp,
        _inserted_timestamp,
        modified_timestamp
    FROM
        streamline
        
)
SELECT
    tx_id,
    block_height,
    block_timestamp,
    marketplace,
    nft_collection,
    nft_id,
    buyer, --Will get deprecated
    buyer as buyer_address,
    seller, --Will get deprecated
    seller as seller_address,
    price,
    price_usd,
    currency, -- Will get deprecated
    currency as currency_address,
    currency_symbol,
    tx_succeeded,
    tokenflow,
    counterparties,
    COALESCE (
        nft_sales_id,
        {{ dbt_utils.generate_surrogate_key(['tx_id']) }}
    ) AS ez_nft_sales_id,
    COALESCE (
        inserted_timestamp,
        _inserted_timestamp
    ) AS inserted_timestamp,
    COALESCE (
        modified_timestamp,
        _inserted_timestamp
    ) AS modified_timestamp
FROM
    FINAL
