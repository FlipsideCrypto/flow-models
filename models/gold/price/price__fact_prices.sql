{{ config(
    materialized = 'view',
    tag = ['scheduled']
) }}

WITH token_labels AS (

    SELECT
        token,
        UPPER(symbol) AS symbol,
        token_contract
    FROM
        {{ ref('seeds__token_labels') }}
),
prices AS (
    SELECT
        recorded_at AS TIMESTAMP,
        token,
        UPPER(symbol) AS symbol,
        price_usd,
        source
    FROM
        {{ this.database }}.silver.prices
),
prices_swaps_cw AS (
    SELECT
        tx_id,
        block_timestamp AS TIMESTAMP,
        token_contract,
        swap_price AS price_usd,
        source,
        _inserted_timestamp,
        NULL AS prices_swaps_id,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM    
        {{ ref('silver__prices_swaps') }}
),
prices_swaps_s AS (
    SELECT
        prices_swaps_id,
        tx_id,
        block_timestamp AS TIMESTAMP,
        token_contract,
        swap_price AS price_usd,
        source,
        _inserted_timestamp,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        {{ ref('silver__prices_swaps_s') }}
),
viewnion AS (
    SELECT
        TIMESTAMP,
        p.token,
        p.symbol,
        l.token_contract,
        price_usd,
        source,
        NULL AS tx_id,
        NULL AS prices_swaps_id,
        NULL AS _inserted_timestamp,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        prices p
        LEFT JOIN token_labels l USING (symbol)
    UNION ALL
    SELECT
        TIMESTAMP,
        l.token,
        l.symbol,
        ps.token_contract,
        price_usd,
        source,
        tx_id,
        prices_swaps_id,
        _inserted_timestamp,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        prices_swaps_cw ps
        LEFT JOIN token_labels l USING (token_contract)
    UNION ALL
    SELECT
        TIMESTAMP,
        l.token,
        l.symbol,
        pss.token_contract,
        price_usd,
        source,
        tx_id,
        prices_swaps_id,
        _inserted_timestamp,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        prices_swaps_s pss
        LEFT JOIN token_labels l USING (token_contract)
)
SELECT
        TIMESTAMP,
        token,
        symbol,
        token_contract,
        price_usd,
        source,
        tx_id,
        COALESCE (
            prices_swaps_id,
            {{ dbt_utils.generate_surrogate_key(['TIMESTAMP','TOKEN', 'TOKEN_CONTRACT', 'SOURCE']) }}
        ) AS price_act_prices,
        COALESCE (
            inserted_timestamp,
            TIMESTAMP
        ) AS inserted_timestamp,
        COALESCE (
            modified_timestamp,
            TIMESTAMP
        ) AS modified_timestamp
FROM
    viewnion
WHERE
    TIMESTAMP IS NOT NULL
qualify ROW_NUMBER() over (
        PARTITION BY TIMESTAMP, TOKEN, TOKEN_CONTRACT, SOURCE
        ORDER BY
            _inserted_timestamp DESC
    ) = 1