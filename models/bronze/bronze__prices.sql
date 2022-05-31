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
            '12182',
            -- Blocto Token
            '15139',
            -- Starly
            'flow',
            'revv',
            'starly',
            'blocto-token'
        )
)
SELECT
    *
FROM
    token_prices
