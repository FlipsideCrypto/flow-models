{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'REWARDS' }} },
    tags = ['scheduled_non_core']
) }}

SELECT
    item_id,
    created_at,
    additional_media,
    animation_url,
    artist_info,
    asset_nr,
    attributes,
    auction_id,
    auction_items,
    burn_to_redeem_details,
    burn_to_redeem_type,
    burned_at,
    contract_address,
    currency_address,
    currency_decimals,
    deleted_at,
    description,
    does_require_w9,
    enable_burn_to_redeem,
    image_url,
    is_listed,
    is_phygital_item,
    is_price_in_usd,
    is_redeemable_separate_from_purchase,
    listing_ends_at,
    listing_starts_at,
    listing_type,
    loyalty_currency,
    loyalty_currency_id,
    metadata_uri,
    mint_status,
    minting_contract,
    minting_contract_asset_mint_status,
    minting_contract_id,
    NAME,
    network,
    organization_id,
    per_user_mint_limit,
    per_user_mint_min_limit,
    pre_reveal_media,
    price,
    quantity,
    quantity_minted,
    redeemable_until,
    revealed_at,
    shipping_price,
    shipping_within,
    status_tracker,
    token_id,
    token_type,
    updated_at,
    website_id,
    response_json,
    storefront_item_id AS dim_storefront_items_id,
    inserted_timestamp,
    modified_timestamp,
    _inserted_timestamp AS request_timestamp
FROM
    {{ ref('silver_api__storefront_items') }}
