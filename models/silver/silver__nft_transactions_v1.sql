{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_ingested_at::DATE, block_timestamp::DATE'],
    unique_key = 'tx_id'
) }}
-- v1 = NFTStorefront transactions & FLOW as Currency
-- so, any and all ListingCompleted sales that use FLOW as currency
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
),
purchase_data AS (
    SELECT
        tx_id,
        event_contract AS currency,
        event_data :amount :: DOUBLE AS amount,
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
),
nft_sales AS (
    SELECT
        *
    FROM
        listing_data
        LEFT JOIN purchase_data USING (tx_id)
        LEFT JOIN seller_data USING (tx_id)
        LEFT JOIN deposit_data USING (tx_id)
    WHERE
        purchased = TRUE
),
step_data AS (
    SELECT
        tx_id,
        event_index,
        event_type,
        event_data
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                nft_sales
            WHERE
                currency = 'A.1654653399040a61.FlowToken'
        )
        AND event_type IN (
            'TokensWithdrawn',
            'TokensDeposited'
        )
),
counterparty_data AS (
    SELECT
        tx_id,
        ARRAY_AGG(OBJECT_CONSTRUCT(event_type, event_data)) within GROUP (
            ORDER BY
                event_index
        ) AS tokenflow,
        ARRAY_SIZE(tokenflow) AS steps,
        ARRAY_AGG(event_type) within GROUP (
            ORDER BY
                event_index
        ) AS action,
        ARRAY_AGG(event_data) within GROUP (
            ORDER BY
                event_index
        ) AS step_data,
        ARRAY_AGG(COALESCE(event_data :to, event_data :from) :: STRING) within GROUP (
            ORDER BY
                event_index
        ) AS counterparties
    FROM
        step_data
    GROUP BY
        1
),
FINAL AS (
    SELECT
        *
    FROM
        nft_sales
        LEFT JOIN counterparty_data USING (tx_id)
)
SELECT
    *
FROM
    FINAL
