{{ config (
    materialized = 'view',
    tags = ['scheduled']
) }}

WITH chainwalkers AS (

    SELECT
        event_contract,
        contract_name,
        account_address
    FROM
        {{ ref('silver__contract_labels') }}
),
streamline AS (
    SELECT
        event_contract,
        contract_name,
        account_address
    FROM
        {{ ref('silver__contract_labels_s') }}
)
SELECT
    *
FROM
    chainwalkers
UNION
SELECT
    *
FROM
    streamline
