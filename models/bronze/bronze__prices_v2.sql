{{ config(
    materialized = 'view'
) }}

WITH coingecko AS (

    SELECT
        'coingecko' AS provider,
        id :: STRING AS id,
        recorded_hour,
        OPEN,
        high,
        low,
        CLOSE,
        _inserted_timestamp
    FROM
        {{ source(
            'crosschain_v2',
            'hourly_prices_coin_gecko'
        ) }}
),
coinmarketcap AS (
    SELECT
        'coinmarketcap' AS provider,
        id :: STRING AS id,
        recorded_hour,
        OPEN,
        high,
        low,
        CLOSE,
        _inserted_timestamp
    FROM
        {{ source(
            'crosschain_v2',
            'hourly_prices_coin_market_cap'
        ) }}
),
token_prices AS (
    SELECT
        *
    FROM
        coingecko
    UNION ALL
    SELECT
        *
    FROM
        coinmarketcap
)
SELECT
    *
FROM
    token_prices
WHERE
    -- numeric ids are cmc, alpha are coingecko
    id IN (
        '4558',
        -- Flow
        '6993',
        -- Revv
        '8075',
        -- Rally
        '12182',
        -- Blocto Token
        '15139',
        -- Starly
        '15194',
        -- Sportium
        'flow',
        'rally-2',
        'revv',
        'sportium',
        'starly',
        'blocto-token'
    )
    AND provider IS NOT NULL -- tokens on increment that are not on either proider:
    -- my
    -- ozone
    -- sdm
    -- stFLOVATAR
    -- thul
    -- ce tokens
