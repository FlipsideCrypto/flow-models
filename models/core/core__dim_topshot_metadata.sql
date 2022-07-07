{{ config (
    materialized = 'view'
) }}

WITH topshot AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_topshot_metadata') }}
)
SELECT
    *
FROM
    topshot
