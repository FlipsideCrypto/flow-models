{{ config(
    materialized = 'view'
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ ref('gold__events') }}
)
SELECT
    *
FROM
    events
