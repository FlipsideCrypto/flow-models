{{ config(
    materialized = 'view',
    tags = ['scheduled']
) }}


WITH token_prices AS (

    SELECT
        provider,
        asset_id,
        recorded_hour,
        OPEN,
        high,
        low,
        CLOSE,
        _inserted_timestamp
    FROM
        {{ ref('bronze__complete_provider_prices') }}
)
SELECT
    *
FROM
    token_prices
WHERE
    asset_id IN ( -- numeric ids are cmc, alpha are coingecko
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
