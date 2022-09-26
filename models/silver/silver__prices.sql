{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_at::DATE'],
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
        COALESCE(
            NAME,
            INITCAP(SPLIT(asset_id, '-') [0])
        ) AS token,
        SPLIT(
            symbol,
            '$'
        ) AS symbol_split,
        symbol_split [array_size(symbol_split) - 1] :: STRING AS symbol,
        price,
        provider AS source
    FROM
        token_prices
),
adj_token_names AS (
    SELECT
        recorded_at,
        asset_id,
        CASE
            WHEN token LIKE 'Flow (%' THEN 'Flow'
            WHEN token = 'Blocto Token' THEN 'Blocto'
            ELSE token
        END AS token,
        COALESCE(
            symbol,
            CASE
                WHEN token = 'Flow' THEN 'FLOW'
                WHEN token = 'Blocto' THEN 'BLT'
                WHEN token = 'Starly' THEN 'STARLY'
                ELSE 'Error'
            END
        ) AS symbol,
        price,
        source
    FROM
        prices
),
FINAL AS (
    SELECT
        recorded_at,
        asset_id,
        token,
        symbol,
        price AS price_usd,
        source
    FROM
        adj_token_names
)
SELECT
    *
FROM
    FINAL
