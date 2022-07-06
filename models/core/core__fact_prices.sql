{{ config(
    materialized = 'view'
) }}

WITH prices AS (

    SELECT
        recorded_at AS TIMESTAMP,
        token,
        symbol,
        price_usd,
        source
    FROM
        {{ ref('silver__prices') }}
),
prices_swaps AS (
    SELECT
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
        token,
        price_usd,
        source
    FROM
        prices
    UNION
    SELECT
        TIMESTAMP,
        token_contract AS token,
        price_usd,
        source
    FROM
        prices_swaps
)
SELECT
    *
FROM
    viewnion
