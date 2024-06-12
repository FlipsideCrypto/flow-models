{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id',
    tags = ['nft', 'scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH topshot AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_topshot_sales_s') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
secondary_mkts AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_transactions_secondary_market_s') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
giglabs AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_giglabs_s') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
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
        _inserted_timestamp,
        _partition_by_block_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','seller', 'buyer', 'nft_collection', 'nft_id']
        ) }} AS nft_sales_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id,
        tokenflow,
        counterparties
    FROM
        topshot
    UNION ALL
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
        _inserted_timestamp,
        _partition_by_block_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','seller', 'buyer', 'nft_collection', 'nft_id']
        ) }} AS nft_sales_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id,
        tokenflow,
        counterparties
    FROM
        secondary_mkts
    UNION ALL
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
        _inserted_timestamp,
        _partition_by_block_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','seller', 'buyer', 'nft_collection', 'nft_id']
        ) }} AS nft_sales_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id,
        tokenflow,
        counterparties
    FROM
        giglabs
)
SELECT
    *
FROM
    combo
