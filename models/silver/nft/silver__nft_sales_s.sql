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
        {{ ref('silver__nft_transactions_secondary_market_s') }}

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
giglabs AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_giglabs_s') }}

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
        _inserted_timestamp,
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
        _inserted_timestamp,
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
        _inserted_timestamp,
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
),
-- TODO: Transaction fees are not included in the transaction data
-- Transaction fees are included in the event data but not always
prices_raw AS (
    SELECT
        HOUR,
        symbol,
        token_address,
        decimals,
        price AS hourly_prices
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        token_address IN (
            SELECT
                DISTINCT currency
            FROM
                combo
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                {{this}}
        )
),
FINAL AS (
    SELECT
        tx_id,
        block_height,
        block_timestamp,
        marketplace,
        nft_collection,
        nft_id,
        buyer,
        --buyer as buyer_address,
        seller,
        --seller as seller_address,
        price,
        IFF(
            p.decimals IS NULL,
            0,
            price * COALESCE(
                hourly_prices,
                0
            )
        ) AS price_usd,
        currency,
        -- currency as currency_address,
        COALESCE(
            p.symbol,
            NULL
        ) AS currency_symbol,
        tx_succeeded,
        _inserted_timestamp,
        nft_sales_id,
        inserted_timestamp,
        modified_timestamp,
        _invocation_id,
        tokenflow,
        counterparties
    FROM
        combo AS b
    LEFT JOIN prices_raw p
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
    AND currency = p.token_address     
)


SELECT
    *
FROM
    FINAL
