{{ config(
    materialized = 'view',
    tag = ['scheduled']
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
