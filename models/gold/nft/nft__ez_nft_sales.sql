{{ config(
    materialized = 'view',
    tags = ['nft', 'ez', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }} }
) }}

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
    COALESCE (
        nft_sales_id,
        {{ dbt_utils.generate_surrogate_key(['tx_id']) }}
    ) AS ez_nft_sales_id,
    inserted_timestamp,
    _inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__nft_sales_s') }}
