{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_ingested_at::DATE, block_timestamp::DATE'],
    unique_key = 'tx_id'
) }}
-- v3 = same as v2 but no amount = 0 condition
-- ultimately no difference in the data from v2 to v3 so the diff conditons do not matter
WITH silver_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}

{% if is_incremental() %}
WHERE
    _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
blocto_sales AS (
    SELECT
        tx_id
    FROM
        silver_events
    WHERE
        event_type = 'TokensDeposited'
        AND LOWER(
            event_data :to
        ) :: STRING = LOWER('0x77E38C96FDA5C5C5')
),
listing_data AS (
    SELECT
        *,
        event_contract AS marketplace_contract,
        event_data :nftID :: STRING AS nft_id_listing,
        event_data :nftType :: STRING AS nft_collection_listing,
        event_data :purchased :: BOOLEAN AS purchased -- false indicates a listing was cancelled while true is when the order was executed
    FROM
        silver_events
    WHERE
        event_type = 'ListingCompleted'
        AND event_contract = 'A.4eb8a10cb9f87357.NFTStorefront' -- general storefront
        AND tx_id IN (
            SELECT
                tx_id
            FROM
                blocto_sales
        )
),
purchase_data AS (
    SELECT
        tx_id,
        event_contract AS currency,
        event_data :amount :: NUMBER AS amount,
        event_data :from :: STRING AS buyer_purchase
    FROM
        silver_events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                listing_data
        )
        AND event_index = 0
),
seller_data AS (
    SELECT
        tx_id,
        event_contract AS nft_collection_seller,
        event_data :from :: STRING AS seller,
        event_data :id :: STRING AS nft_id_seller
    FROM
        silver_events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                listing_data
        )
        AND event_type = 'Withdraw'
),
deposit_data AS (
    SELECT
        tx_id,
        event_contract AS nft_collection_deposit,
        event_data :id :: STRING AS nft_id_deposit,
        event_data :to :: STRING AS buyer_deposit
    FROM
        silver_events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                listing_data
        )
        AND event_type = 'Deposit'
)
SELECT
    *
FROM
    listing_data
    LEFT JOIN purchase_data USING (tx_id)
    LEFT JOIN seller_data USING (tx_id)
    LEFT JOIN deposit_data USING (tx_id)
WHERE
    purchased = TRUE
