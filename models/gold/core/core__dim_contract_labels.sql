{{ config (
    materialized = 'view',
    tags = ['scheduled']
) }}

WITH chainwalkers AS (

    SELECT
        event_contract,
        contract_name,
        account_address,
        _inserted_timestamp
    FROM
        {{ ref('silver__contract_labels') }}
),
streamline AS (
    SELECT
        event_contract,
        contract_name,
        account_address,
        _inserted_timestamp
    FROM
        {{ ref('silver__contract_labels_s') }}
),
FINAL AS (
    SELECT
        *
    FROM
        chainwalkers
    UNION ALL
    SELECT
        *
    FROM
        streamline
)
SELECT
    *
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY event_contract
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
