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
        {{ ref('silver__nft_sales') }}

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
purchase_amt AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        _inserted_timestamp,
        'A.ead892083b3e2c6c.DapperUtilityCoin' AS currency,
        event_data :amount :: DOUBLE AS amount,
        event_data :from :: STRING AS forwarded_from
    FROM
        gig_sales_events
    WHERE
        event_type = 'ForwardedDeposit'
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
FINAL AS (
    SELECT
        p.tx_id,
        p.block_timestamp,
        p.block_height,
        p._inserted_timestamp,
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
        purchase_amt p
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
)
SELECT
    *
FROM
    FINAL
