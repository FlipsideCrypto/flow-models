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
        _ingested_at
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
        _ingested_at
    FROM
        moment_data
        LEFT JOIN currency_data USING (tx_id)
        LEFT JOIN nft_data USING (tx_id)
)
SELECT
    *
FROM
    combo
