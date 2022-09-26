{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id',
    tags = ['nft']
) }}

WITH silver_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}
        -- WHERE
        --     event_data :: STRING != '{}'

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
sale_trigger AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_contract AS marketplace,
        event_data,
        COALESCE(
            event_data :purchased :: BOOLEAN,
            TRUE
        ) AS is_purchased,
        _ingested_at,
        _inserted_timestamp
    FROM
        silver_events
    WHERE
        is_purchased
        AND -- each market uses a slightly different sale trigger
        (
            (
                event_contract = 'A.64f83c60989ce555.ChainmonstersMarketplace'
                AND event_type = 'CollectionRemovedSaleOffer'
            )
            OR (
                event_contract = 'A.921ea449dffec68a.FlovatarMarketplace'
                AND event_type IN (
                    'FlovatarPurchased',
                    'FlovatarComponentPurchased'
                )
            )
            OR (
                event_contract = 'A.09e03b1f871b3513.TheFabricantMarketplace'
                AND event_type = 'NFTPurchased'
            )
            OR (
                event_contract = 'A.2162bbe13ade251e.MatrixMarketOpenOffer'
                AND event_type = 'OfferCompleted'
            )
            OR (
                event_contract = 'A.4eb8a10cb9f87357.NFTStorefront' -- general storefront
                AND event_type = 'ListingCompleted'
            )
            OR (
                event_contract = 'A.85b075e08d13f697.OlympicPinMarket'
                AND event_type = 'PiecePurchased'
            )
            OR (
                event_contract = 'A.5b82f21c0edf76e3.StarlyCardMarket'
                AND event_type = 'CollectionRemovedSaleOffer'
            )
        )
),
excl_multi_buys AS (
    SELECT
        tx_id,
        COUNT(1) AS record_count
    FROM
        sale_trigger
    GROUP BY
        1
    HAVING
        record_count = 1
),
omit_nft_nontransfers AS (
    SELECT
        tx_id,
        ARRAY_AGG(
            DISTINCT event_type
        ) AS events,
        ARRAY_SIZE(
            array_intersection(
                ['Deposit', 'Withdraw', 'FlovatarSaleWithdrawn', 'FlovatarComponentSaleWithdrawn'],
                events
            )
        ) = 2 AS nft_transferred,
        count_if(
            event_type = 'Deposit'
        ) AS nft_deposits
    FROM
        silver_events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                excl_multi_buys
        )
    GROUP BY
        1
    HAVING
        nft_deposits = 1
),
first_token_withdraw AS (
    SELECT
        tx_id,
        MIN(event_index) AS min_index
    FROM
        silver_events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                omit_nft_nontransfers
            WHERE
                nft_transferred
        )
        AND event_type = 'TokensWithdrawn'
    GROUP BY
        1
),
-- 3 most important events are the first TokenWithdraw, then Withdraw and Deposit (NFT movement)
token_withdraw_event AS (
    SELECT
        tx_id,
        event_contract AS currency,
        event_data :amount :: DOUBLE AS amount,
        event_data :from :: STRING AS buyer_purchase,
        min_index
    FROM
        silver_events
        LEFT JOIN first_token_withdraw USING (tx_id)
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                omit_nft_nontransfers
            WHERE
                nft_transferred
        )
        AND event_index = min_index
        AND event_type = 'TokensWithdrawn'
),
nft_withdraw_event_seller AS (
    SELECT
        tx_id,
        event_index AS event_index_seller,
        event_contract AS nft_collection_seller,
        COALESCE(
            event_data :from,
            event_data :address
        ) :: STRING AS seller,
        COALESCE(
            event_data :id,
            event_data :tokenId
        ) :: STRING AS nft_id_seller
    FROM
        silver_events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                omit_nft_nontransfers
            WHERE
                nft_transferred
        )
        AND event_type IN (
            'Withdraw',
            'FlovatarSaleWithdrawn',
            'FlovatarComponentSaleWithdrawn' -- if adding anything new, don't forget about omit_nft_nontransfers check!
        )
),
nft_deposit_event_buyer AS (
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
                omit_nft_nontransfers
            WHERE
                nft_transferred
        )
        AND event_type = 'Deposit'
),
nft_sales AS (
    SELECT
        e.tx_id,
        e.block_timestamp,
        e.block_height,
        e.tx_succeeded,
        e.is_purchased,
        e.marketplace,
        w.currency,
        w.amount,
        w.buyer_purchase,
        s.nft_collection_seller,
        s.seller,
        s.nft_id_seller,
        b.nft_collection_deposit,
        b.nft_id_deposit,
        b.buyer_deposit,
        e._ingested_at,
        e._inserted_timestamp
    FROM
        sale_trigger e
        LEFT JOIN token_withdraw_event w USING (tx_id)
        LEFT JOIN nft_withdraw_event_seller s USING (tx_id)
        LEFT JOIN nft_deposit_event_buyer b USING (tx_id)
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                omit_nft_nontransfers
            WHERE
                nft_transferred
        )
),
step_data AS (
    SELECT
        tx_id,
        event_index,
        event_type,
        event_data
    FROM
        silver_events
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
            'ForwardedDeposit',
            'RoyaltyDeposited'
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
        marketplace,
        nft_collection_deposit AS nft_collection,
        nft_id_seller AS nft_id,
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
