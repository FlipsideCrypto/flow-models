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
    *
FROM
    prices
