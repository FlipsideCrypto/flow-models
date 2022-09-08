{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id',
    tags = ['nft']
) }}

WITH topshot AS (

    SELECT
        *
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
secondary_mkts AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_transactions_secondary_market') }}

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
        _ingested_at,
        _inserted_timestamp,
        tokenflow,
        counterparties
    FROM
        topshot
    UNION
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
        _ingested_at,
        _inserted_timestamp,
        tokenflow,
        counterparties
    FROM
        secondary_mkts
)
SELECT
    *
FROM
    combo
