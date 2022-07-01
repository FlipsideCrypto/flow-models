{{ config(
    materialized = 'view'
) }}

WITH token_prices AS (

    SELECT
        *
    FROM
        {{ source(
            'silver',
            'prices_v2'
        ) }}
    WHERE
        asset_id IN (
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
