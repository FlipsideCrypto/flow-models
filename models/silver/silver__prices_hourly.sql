{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['recorded_hour'],
    unique_key = "concat_ws( '-', recorded_at, id )"
) }}

WITH token_prices AS (

    SELECT
        *
    FROM
        {{ ref('bronze__prices_v2') }}

{% if is_incremental() %}
WHERE
    recorded_hour >= (
        SELECT
            MAX(recorded_hour)
        FROM
            {{ this }}
    )
{% endif %}
),
prices AS (
    SELECT
        recorded_hour,
        id AS asset_id,
        INITCAP(SPLIT(asset_id, '-') [0]) AS token,
        OPEN,
        high,
        low,
        CLOSE,
        provider
    FROM
        token_prices
),
adj_token_names AS (
    SELECT
        recorded_hour,
        asset_id,
        CASE
            WHEN token LIKE 'Flow (%' THEN 'Flow'
            WHEN token = 'Blocto Token' THEN 'Blocto'
            WHEN token = '4558' THEN 'Flow'
            WHEN token = '6993' THEN 'Revv'
            WHEN token = '8075' THEN 'Rally'
            WHEN token = '12182' THEN 'Blocto'
            WHEN token = '15139' THEN 'Starly'
            WHEN token = '15194' THEN 'Sportium'
            ELSE token
        END AS token,
        OPEN,
        high,
        low,
        CLOSE,
        provider
    FROM
        prices
),
FINAL AS (
    SELECT
        recorded_hour,
        asset_id AS id,
        token,
        OPEN,
        high,
        low,
        CLOSE,
        provider
    FROM
        adj_token_names
)
SELECT
    *
FROM
    FINAL
