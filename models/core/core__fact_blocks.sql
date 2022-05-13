{{ config(
    materialized = 'view'
) }}

WITH blocks AS (

    SELECT
        *
    FROM
        {{ ref('gold__blocks') }}
)
SELECT
    *
FROM
    blocks
