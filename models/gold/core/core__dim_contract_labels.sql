{{ config (
    materialized = 'view',
    tags = ['scheduled']
) }}

WITH contract_labels AS (

    SELECT
        *
    FROM
        {{ ref('silver__contract_labels') }}
)
SELECT
    *
FROM
    contract_labels
