{{ config(
    materialized = 'view'
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
prices_swaps AS (
    SELECT
        tx_id,
        block_timestamp AS TIMESTAMP,
        token_contract,
        swap_price AS price_usd,
        source
    FROM
        {{ ref('silver__prices_swaps') }}
),
viewnion AS (
    SELECT
        TIMESTAMP,
        p.token,
        p.symbol,
        l.token_contract,
        price_usd,
        source,
        NULL as tx_id
    FROM
        prices p
        LEFT JOIN token_labels l USING (symbol)
    UNION
    SELECT
        TIMESTAMP,
        l.token,
        l.symbol,
        ps.token_contract,
        price_usd,
        source,
        tx_id
    FROM
        prices_swaps ps
        LEFT JOIN token_labels l USING (token_contract)
)
SELECT
    *
FROM
    viewnion
