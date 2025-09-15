{{ config (
    materialized = "view",
    tags = ['full_evm_test']
) }}

SELECT
    *
FROM
    {{ ref('testnet__fact_evm_transactions') }}