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
fabricant_mkt_txs AS (
    SELECT
        *
    FROM
        silver_events
    WHERE
        event_contract = 'A.09e03b1f871b3513.TheFabricantMarketplace'
),
fabricant_events AS (
    SELECT
        *
    FROM
        silver_events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                fabricant_mkt_txs
        )
),
nft_purchase AS (
    SELECT
        tx_id,
        event_index,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_contract AS event_contract_purchase,
        event_data :buyer :: STRING AS buyer_purchase,
        event_data :listingID :: STRING AS listing_id,
        event_data :seller :: STRING AS seller_purchase,
        _ingested_at,
        _inserted_timestamp
    FROM
        fabricant_events
    WHERE
        event_type = 'NFTPurchased'
),
cost AS (
    SELECT
        tx_id,
        event_index,
        block_timestamp,
        event_contract AS event_contract_currency,
        event_data :amount :: FLOAT AS amount,
        event_data :from :: STRING AS buyer_cost
    FROM
        fabricant_events
    WHERE
        event_index = 0
        AND event_type = 'TokensWithdrawn'
),
seller_data AS (
    SELECT
        tx_id,
        event_index,
        block_timestamp,
        event_contract AS event_contract_withdraw,
        event_data :from :: STRING AS seller,
        event_data :id :: STRING AS nft_id
    FROM
        fabricant_events
    WHERE
        event_type = 'Withdraw'
),
buyer_data AS (
    SELECT
        tx_id,
        event_index,
        block_timestamp,
        event_contract AS event_contract_deposit,
        event_data :to :: STRING AS buyer,
        event_data :id :: STRING AS nft_id
    FROM
        fabricant_events
    WHERE
        event_type = 'Deposit'
),
fabricant_nft_sales AS (
    SELECT
        p.block_timestamp,
        p.block_height,
        p.tx_id,
        p.tx_succeeded,
        p.event_index AS purchase_event_index,
        event_contract_purchase,
        buyer_purchase,
        listing_id,
        seller_purchase,
        C.event_index AS event_index_cost,
        event_contract_currency,
        amount,
        buyer_cost,
        s.event_index AS event_index_withdraw,
        event_contract_withdraw,
        seller,
        s.nft_id AS nft_id_withdraw,
        b.event_index AS event_index_deposit,
        event_contract_deposit,
        buyer,
        b.nft_id AS nft_id_deposit,
        _ingested_at,
        _inserted_timestamp
    FROM
        nft_purchase p
        LEFT JOIN cost C USING (tx_id)
        LEFT JOIN seller_data s USING (tx_id)
        LEFT JOIN buyer_data b USING (tx_id)
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
                fabricant_nft_sales
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
        block_timestamp,
        block_height,
        fns.tx_id,
        event_contract_purchase AS marketplace,
        listing_id,
        event_contract_currency AS currency,
        amount AS price,
        event_contract_withdraw AS nft_collection,
        seller,
        nft_id_withdraw AS nft_id,
        buyer,
        cd.tokenflow,
        cd.steps AS num_steps,
        cd.action AS step_action,
        cd.step_data,
        cd.counterparties,
        tx_succeeded,
        _ingested_at,
        _inserted_timestamp
    FROM
        fabricant_nft_sales fns
        LEFT JOIN counterparty_data cd USING (tx_id)
)
SELECT
    *
FROM
    FINAL
