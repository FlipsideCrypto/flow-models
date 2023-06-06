{{ config(
    materialized = 'view'
) }}

WITH validators AS (

    SELECT
        *
    FROM
        {{ ref('silver__validator_labels') }}
)
SELECT
    *
FROM
    validators
