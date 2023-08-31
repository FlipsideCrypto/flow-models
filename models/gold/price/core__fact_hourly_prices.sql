{{ config(
    materialized = 'view',
    tags = ['scheduled']
) }}

WITH api AS (

    SELECT
        recorded_hour,
        id,
        token,
        OPEN,
        high,
        low,
        CLOSE,
        provider
    FROM
        {{ ref('silver__prices_hourly') }}
),
swaps AS (
    SELECT
        recorded_hour,
        id,
        CASE
            WHEN id = 'A.1654653399040a61.FlowToken' THEN 'Flow'
            WHEN id = 'A.cfdd90d4a00f7b5b.TeleportedTetherToken' THEN 'USDT'
            WHEN id = 'A.3c5959b568896393.FUSD' THEN 'FUSD'
            WHEN id = 'A.0f9df91c9121c460.BloctoToken' THEN 'Blocto'
            WHEN id = 'A.d01e482eb680ec9f.REVV' THEN 'Revv'
            WHEN id = 'A.b19436aae4d94622.FiatToken' THEN 'USDC'
            WHEN id = 'A.142fa6570b62fd97.StarlyToken' THEN 'Starly'
            WHEN id = 'A.475755d2c9dccc3a.TeleportedSportiumToken' THEN 'Sportium'
            ELSE NULL -- will trigger alert if swaps model picks up another token
        END AS token,
        OPEN,
        high,
        low,
        CLOSE,
        provider
    FROM
        {{ ref('silver__prices_swaps_hourly') }}
),
FINAL AS (
    SELECT
        *
    FROM
        api
    UNION
    SELECT
        *
    FROM
        swaps
)
SELECT
    *
FROM
    FINAL
