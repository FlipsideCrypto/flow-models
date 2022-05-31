{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_at'],
    unique_key = "concat_ws( '-', recorded_at, asset_id )"
) }}

WITH token_prices AS (

    SELECT
        *
    FROM
        {{ ref('bronze__prices') }}

{% if is_incremental() %}
WHERE
    recorded_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
prices AS (
    SELECT
        recorded_at,
        asset_id,
        NAME AS token,
        symbol,
        price,
        market_cap,
        provider AS source
    FROM
        token_prices
)
SELECT
    *
FROM
    prices
