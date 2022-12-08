{{ config(
    materialized = 'view'
) }}

WITH token_prices AS (

    SELECT
        *
    FROM
        {{ source(
            'crosschain_v2',
            'fact_hourly_prices'
        ) }}
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
        AND provider IS NOT NULL
)
SELECT
    *
FROM
    token_prices 
    
-- tokens on increment that are not on either proider:
-- my
-- ozone
-- sdm
-- stFLOVATAR
-- thul
-- ce tokens
