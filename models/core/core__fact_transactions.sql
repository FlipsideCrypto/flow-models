{{ config(
    materialized = 'view'
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('gold__transactions') }}
)
SELECT
    *
FROM
    txs
