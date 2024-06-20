{{ config (
    materialized = 'view',
    tags = ['scheduled']
) }}

SELECT
    event_contract_id,
    event_contract,
    contract_name,
    account_address,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    COALESCE (
        event_contract_id,
        {{ dbt_utils.generate_surrogate_key(['event_contract']) }}
    ) AS dim_contract_labels_id
FROM
    {{ ref('silver__contract_labels_s') }}
