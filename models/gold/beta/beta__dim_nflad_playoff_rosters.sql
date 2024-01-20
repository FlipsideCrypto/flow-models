{{ config(
    materialized = 'view'
) }}

WITH rookies AS (

    SELECT
        *,
        'rookie' AS campaign
    FROM
        {{ ref('seeds__nflad_playoff_rookies') }}
),
vets AS (
    SELECT
        *,
        'vet' AS campaign
    FROM
        {{ ref('seeds__nflad_playoff_vets') }}
)
SELECT
    *
FROM
    rookies
UNION ALL
SELECT
    *
FROM
    vets
