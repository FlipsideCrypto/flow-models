{{ config(
    materialized = 'incremental',
    unique_key = "item_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE'],
    post_hook = [ "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(storefront_item_id)" ],
    tags = ['streamline_non_core']
) }}

WITH silver_responses AS (

    SELECT
        partition_key,
        item_id,
        created_at,
        INDEX,
        DATA,
        _inserted_timestamp
    FROM
        {{ ref('silver_api__minting_assets') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

)
SELECT
    item_id,
    created_at,

    DATA :additionalMedia :: variant AS additional_media,
    DATA :animationUrl :: STRING AS animation_url,
    DATA :artistInfo :: variant AS artist_info,
    DATA :assetNr :: STRING AS asset_nr,
    DATA :attributes :: variant AS attributes,
    DATA :auctionId :: STRING AS auction_id,
    DATA :auctionItems :: variant AS auction_items,
    DATA :burnToRedeemDetails :: variant AS burn_to_redeem_details,
    DATA :burnToRedeemType :: STRING AS burn_to_redeem_type,
    DATA :burnedAt :: timestamp_ntz AS burned_at,
    DATA :contractAddress :: STRING AS contract_address,
    DATA :currencyAddress :: STRING AS currency_address,
    DATA :currencyDecimals :: INTEGER AS currency_decimals,
    DATA :deletedAt :: timestamp_ntz AS deleted_at,
    DATA :description :: STRING AS description,
    DATA :doesRequireW9 :: BOOLEAN AS does_require_w9,
    DATA :enableBurnToRedeem :: BOOLEAN AS enable_burn_to_redeem,
    DATA :imageUrl :: STRING AS image_url,
    DATA :isListed :: BOOLEAN AS is_listed,
    DATA :isPhygitalItem :: BOOLEAN AS is_phygital_item,
    DATA :isPriceInUSD :: BOOLEAN AS is_price_in_usd,
    DATA :isRedeemableSeparateFromPurchase :: BOOLEAN AS is_redeemable_separate_from_purchase,
    DATA :listingEndsAt :: timestamp_ntz AS listing_ends_at,
    DATA :listingStartsAt :: timestamp_ntz AS listing_starts_at,
    DATA :listingType :: STRING AS listing_type,
    DATA :loyaltyCurrency :: STRING AS loyalty_currency,
    DATA :loyaltyCurrencyId :: STRING AS loyalty_currency_id,
    DATA :metadataUri :: STRING AS metadata_uri,
    DATA :mintStatus :: STRING AS mint_status,
    DATA :mintingContract :: VARIANT AS minting_contract,
    DATA :mintingContractAssetMintStatus :: STRING AS minting_contract_asset_mint_status,
    DATA :mintingContractId :: STRING AS minting_contract_id,
    DATA :name :: STRING AS name,
    DATA :network :: STRING AS network,
    DATA :organizationId :: STRING AS organization_id,
    DATA :perUserMintLimit :: INTEGER AS per_user_mint_limit,
    DATA :perUserMintMinLimit :: INTEGER AS per_user_mint_min_limit,
    DATA :preRevealMedia :: variant AS pre_reveal_media,
    DATA :price :: NUMBER AS price,
    DATA :quantity :: INTEGER AS quantity,
    DATA :quantityMinted :: INTEGER AS quantity_minted,
    DATA :redeemableUntil :: timestamp_ntz AS redeemable_until,
    DATA :revealedAt :: timestamp_ntz AS revealed_at,
    DATA :shippingPrice :: NUMBER AS shipping_price,
    DATA :shippingWithin :: STRING AS shipping_within,
    DATA :statusTracker :: STRING AS status_tracker,
    DATA :tokenId :: STRING AS token_id,
    DATA :tokenType :: STRING AS token_type,
    DATA :updatedAt :: timestamp_ntz AS updated_at,
    DATA :websiteId :: STRING AS website_id,
    DATA AS response_json,
    partition_key,
    INDEX,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['item_id', 'partition_key']
    ) }} AS storefront_item_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    silver_responses


qualify(ROW_NUMBER() over (PARTITION BY item_id
ORDER BY
    _inserted_timestamp DESC)) = 1
