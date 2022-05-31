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
            '15139' -- Starly
        )
)
SELECT
    *
FROM
    token_prices
