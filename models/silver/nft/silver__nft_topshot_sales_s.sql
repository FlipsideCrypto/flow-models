{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id',
    tags = ['nft', 'scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH silver_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_events') }}
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
moment_data AS (
    SELECT
        block_height,
        block_timestamp,
        tx_id,
        event_contract :: STRING AS marketplace,
        event_data :id :: STRING AS nft_id,
        event_data :price :: DOUBLE AS price,
        event_data :seller :: STRING AS seller,
        tx_succeeded,
        _inserted_timestamp
    FROM
        silver_events
    WHERE
        event_type = 'MomentPurchased'
        AND event_contract LIKE 'A.c1e4f4f4c4257510%' -- topshot
),
currency_data AS (
    SELECT
        tx_id,
        event_contract :: STRING AS currency
    FROM
        silver_events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                moment_data
        )
        AND event_index = 0
),
nft_data AS (
    SELECT
        tx_id,
        event_contract :: STRING AS nft_collection,
        event_data :to :: STRING AS buyer
    FROM
        silver_events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                moment_data
        )
        AND event_type = 'Deposit'
),
combo AS (
    SELECT
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
        _inserted_timestamp
    FROM
        moment_data
        LEFT JOIN currency_data USING (tx_id)
        LEFT JOIN nft_data USING (tx_id)
),
step_data AS (
    SELECT
        tx_id,
        event_index,
        event_type,
        event_data
    FROM
        {{ ref('silver__streamline_events') }}
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                combo
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
        ARRAY_AGG(OBJECT_CONSTRUCT(event_type :: STRING, event_data)) within GROUP (
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
        C.tx_id,
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
        _inserted_timestamp,
        cd.tokenflow,
        cd.counterparties
    FROM
        combo C
        LEFT JOIN counterparty_data cd USING (tx_id)
)
SELECT
    *
FROM
    FINAL
