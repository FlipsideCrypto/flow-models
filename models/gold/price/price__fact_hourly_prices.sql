{{ config(
    materialized = 'view',
    tag = ['scheduled']
) }}

WITH api AS (

    SELECT
        NULL AS prices_swaps_hourly_id,
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
swaps_cw AS (
    SELECT
        NULL AS prices_swaps_hourly_id,
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
        provider,
        _inserted_timestamp,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        {{ ref('silver__prices_swaps_hourly') }}
),
swaps_s AS (
    SELECT
        prices_swaps_hourly_id,
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
        provider,
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__prices_swaps_hourly_s') }}
),
FINAL AS (
    SELECT
        *
    FROM
        api
    UNION ALL
    SELECT
        *
    FROM
        swaps_cw
    UNION ALL
    SELECT
        *
    FROM
        swaps_s
)
SELECT
    COALESCE (
        prices_swaps_hourly_id,
        {{ dbt_utils.generate_surrogate_key(['recorded_hour', 'token']) }}
    ) AS prices_swaps_hourly_id,
    recorded_hour,
    id,
    token,
    OPEN,
    high,
    low,
    CLOSE,
    provider,
    COALESCE (
        inserted_timestamp,
        _inserted_timestamp
    ) AS inserted_timestamp,
    COALESCE (
        modified_timestamp,
        _inserted_timestamp
    ) AS modified_timestamp
FROM
    FINAL
WHERE
    recorded_hour IS NOT NULL qualify ROW_NUMBER() over (
        PARTITION BY prices_swaps_hourly_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
