{{ config (
    materialized = 'view',
    tags = ['scheduled']
) }}

WITH chainwalkers AS (

    SELECT
        NULL AS event_contract_id,
        event_contract,
        contract_name,
        account_address,
        _inserted_timestamp,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        {{ ref('silver__contract_labels') }}
),
streamline AS (
    SELECT
        event_contract_id,
        event_contract,
        contract_name,
        account_address,
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp
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
        *,
    FROM
        streamline
)
SELECT
    COALESCE (
        event_contract_id,
        {{ dbt_utils.generate_surrogate_key(['event_contract']) }}
    ) AS event_contract_id,
    event_contract,
    contract_name,
    account_address,
    COALESCE (
        inserted_timestamp,
        _inserted_timestamp
    ) AS inserted_timestamp,
    COALESCE (
        modified_timestamp,
        _inserted_timestamp
    ) AS modified_timestamp
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY event_contract
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
