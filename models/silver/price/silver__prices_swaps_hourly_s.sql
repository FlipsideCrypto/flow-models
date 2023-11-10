{{ config(
    materialized = 'table',
    cluster_by = ['recorded_hour::date'],
    unique_key = "CONCAT_WS('-', recorded_hour, token)",
    tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH swap_prices AS (

    SELECT
        *
    FROM
        {{ ref('silver__prices_swaps_s') }}

),
lowhigh AS (
    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS recorded_hour,
        token_contract AS token,
        MIN(swap_price) AS low,
        MAX(swap_price) AS high,
        COUNT(
            DISTINCT tx_id
        ) AS num_swaps
    FROM
        swap_prices
    GROUP BY
        1,
        2
),
openclose AS (
    SELECT
        DATE_TRUNC(
            'hour',
            block_timestamp
        ) AS recorded_hour,
        token_contract AS token,
        FIRST_VALUE(swap_price) over (
            PARTITION BY recorded_hour,
            token
            ORDER BY
                block_timestamp
        ) AS OPEN,
        LAST_VALUE(swap_price) over (
            PARTITION BY recorded_hour,
            token
            ORDER BY
                block_timestamp
        ) AS CLOSE
    FROM
        swap_prices qualify ROW_NUMBER() over (PARTITION BY concat_ws('-', recorded_hour, token)
    ORDER BY
        recorded_hour DESC) = 1
),
FINAL AS (
    SELECT
        l.recorded_hour,
        l.token as id,
        OPEN,
        high,
        low,
        CLOSE,
        num_swaps,
        'Swaps' as provider
    FROM
        lowhigh l
        LEFT JOIN openclose o USING (
            recorded_hour,
            token
        )
)
SELECT
    *
FROM
    FINAL
