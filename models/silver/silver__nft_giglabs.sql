{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id',
    tags = ['nft']
) }}

WITH events AS (

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
mapped_sales AS (
    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__nft_transactions_secondary_market') }}
    UNION
    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__nft_topshot_sales') }}

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
duc AS (
    SELECT
        DISTINCT tx_id
    FROM
        events
    WHERE
        event_contract = 'A.ead892083b3e2c6c.DapperUtilityCoin'
),
duc_events AS (
    SELECT
        *
    FROM
        events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                duc
        )
        AND tx_id NOT IN (
            SELECT
                tx_id
            FROM
                mapped_sales
        )
),
duc_transfers AS (
    SELECT
        _inserted_timestamp,
        tx_id,
        COUNT(event_type) AS event_count,
        MAX(
            event_index + 1
        ) AS max_index
    FROM
        duc_events
    WHERE
        event_type IN (
            'TokensDeposited',
            'TokensWithdrawn',
            'FeesDeducted',
            'ForwardedDeposit'
        )
    GROUP BY
        _inserted_timestamp,
        tx_id
    HAVING
        event_count = max_index
),
gig_nfts AS (
    SELECT
        *
    FROM
        duc_events
    WHERE
        tx_id NOT IN (
            SELECT
                DISTINCT tx_id
            FROM
                duc_transfers
        )
        AND event_contract ILIKE 'A.329feb3ab062d289%'
        AND event_type IN (
            'Withdraw',
            'Deposit'
        )
),
gig_sales_events AS (
    SELECT
        *
    FROM
        events
    WHERE
        tx_id IN (
            SELECT
                DISTINCT tx_id
            FROM
                gig_nfts
        )
),
missing_contract AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        _inserted_timestamp,
        event_contract AS currency,
        event_data :amount :: DOUBLE AS amount,
        event_data :from :: STRING AS forwarded_from,
        TRUE AS missing
    FROM
        gig_sales_events
    WHERE
        event_index = 0
        AND event_type = 'TokensWithdrawn'
),
purchase_amt AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        _inserted_timestamp,
        'A.ead892083b3e2c6c.DapperUtilityCoin' AS currency,
        event_data :amount :: DOUBLE AS amount,
        event_data :from :: STRING AS forwarded_from,
        FALSE AS missing
    FROM
        gig_sales_events
    WHERE
        event_type = 'ForwardedDeposit'
        AND tx_id NOT IN (
            SELECT
                tx_id
            FROM
                missing_contract
        )
),
triage AS (
    SELECT
        *
    FROM
        missing_contract
    UNION
    SELECT
        *
    FROM
        purchase_amt
),
withdraw_event AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        _inserted_timestamp,
        event_contract AS nft_collection,
        event_data :from :: STRING AS seller,
        event_data :id :: NUMBER AS nft_id
    FROM
        gig_sales_events
    WHERE
        event_type = 'Withdraw'
        AND event_data :from :: STRING != 'null'
),
deposit_event AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        _inserted_timestamp,
        event_contract AS nft_collection,
        event_data :to :: STRING AS buyer,
        event_data :id :: NUMBER AS nft_id
    FROM
        gig_sales_events
    WHERE
        event_type = 'Deposit'
        AND event_data :to :: STRING != 'null'
),
gl_sales AS (
    SELECT
        p.tx_id,
        p.block_timestamp,
        p.block_height,
        p.tx_succeeded,
        p._inserted_timestamp,
        'Gigantik Primary Market' AS marketplace,
        p.missing,
        p.currency,
        p.amount,
        p.forwarded_from,
        w.seller,
        d.buyer,
        w.nft_collection,
        w.nft_id AS withdraw_nft_id,
        d.nft_id AS deposit_nft_id,
        w.nft_collection = d.nft_collection AS collection_check,
        w.nft_id = d.nft_id AS nft_id_check
    FROM
        triage p
        LEFT JOIN withdraw_event w USING (
            tx_id,
            block_timestamp,
            block_height,
            _inserted_timestamp
        )
        LEFT JOIN deposit_event d USING (
            tx_id,
            block_timestamp,
            block_height,
            _inserted_timestamp
        )
),
multi AS (
    SELECT
        tx_id,
        COUNT(
            DISTINCT deposit_nft_id
        ) AS nfts
    FROM
        gl_sales
    WHERE
        nft_id_check
    GROUP BY
        1
),
giglabs_final AS (
    SELECT
        s.tx_id,
        block_timestamp,
        block_height,
        marketplace,
        currency,
        amount / m.nfts AS price,
        seller,
        buyer,
        nft_collection,
        withdraw_nft_id AS nft_id,
        m.nfts,
        tx_succeeded,
        _inserted_timestamp
    FROM
        gl_sales s
        LEFT JOIN multi m USING (tx_id)
    WHERE
        nft_id_check
),
step_data AS (
    SELECT
        tx_id,
        event_index,
        event_type,
        event_data
    FROM
        events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                giglabs_final
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
        s.tx_id,
        block_timestamp,
        block_height,
        marketplace,
        currency,
        price,
        seller,
        buyer,
        nft_collection,
        nft_id,
        nfts,
        tokenflow,
        counterparties,
        tx_succeeded,
        _inserted_timestamp
    FROM
        giglabs_final s
        LEFT JOIN counterparty_data C USING (tx_id)
)
SELECT
    *
FROM
    FINAL
