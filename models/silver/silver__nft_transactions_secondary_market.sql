{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_ingested_at::DATE, block_timestamp::DATE'],
    unique_key = 'tx_id'
) }}

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
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_index AS event_index_listing,
        event_contract AS event_contract_listing,
        event_data AS event_data_listing,
        event_data :nftID :: STRING AS nft_id_listing,
        event_data :nftType :: STRING AS nft_collection_listing,
        event_data :purchased :: BOOLEAN AS purchased_listing,
        _ingested_at,
        _inserted_timestamp
    FROM
        silver_events
    WHERE
        event_type = 'ListingCompleted'
        AND event_contract = 'A.4eb8a10cb9f87357.NFTStorefront' -- general storefront
        AND purchased_listing = TRUE
),
excl_multi_buys AS (
    SELECT
        tx_id,
        COUNT(1) AS record_count
    FROM
        listing_data
    GROUP BY
        1
    HAVING
        record_count = 1
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
                excl_multi_buys
        )
        AND event_index = 0
),
seller_data AS (
    SELECT
        tx_id,
        event_index AS event_index_seller,
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
                excl_multi_buys
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
                excl_multi_buys
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
        tx_id IN (
            SELECT
                tx_id
            FROM
                excl_multi_buys
        )
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
        )
        AND event_type IN (
            'TokensWithdrawn',
            'TokensDeposited',
            'ForwardedDeposit'
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
        ns.tx_id,
        block_timestamp,
        block_height,
        event_contract_listing AS marketplace,
        event_data_listing,
        nft_collection_seller AS nft_collection,
        event_data_listing :storefrontResourceID :: NUMBER AS storefront_id,
        event_data_listing :listingResourceID :: NUMBER AS listing_id,
        nft_id_listing AS nft_id,
        currency,
        amount AS price,
        seller,
        buyer_deposit AS buyer,
        cd.tokenflow,
        cd.steps AS num_steps,
        cd.action AS step_action,
        cd.step_data,
        cd.counterparties,
        tx_succeeded,
        _ingested_at,
        _inserted_timestamp
    FROM
        nft_sales ns
        LEFT JOIN counterparty_data cd USING (tx_id)
)
SELECT
    *
FROM
    FINAL
