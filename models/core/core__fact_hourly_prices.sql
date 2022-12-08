{{ config(
    materialized = 'view'
) }}

WITH FINAL AS (

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
)
SELECT
    *
FROM
    FINAL
