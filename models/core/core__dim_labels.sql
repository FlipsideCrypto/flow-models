{{ config (
    materialized = 'view'
) }}

WITH contract_labels AS (

    SELECT
        *
    FROM
        {{ ref('silver__contract_labels') }}
)
SELECT
    event_contract,
    contract_name,
    account_address
FROM
    contract_labels
