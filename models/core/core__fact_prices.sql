{{ config(
    materialized = 'view'
) }}

WITH prices AS (

    SELECT
        *
    FROM
        {{ ref('silver__prices') }}
)
SELECT
    recorded_at AS TIMESTAMP,
    asset_id,
    token,
    symbol,
    price_usd,
    market_cap,
    source
FROM
    prices
