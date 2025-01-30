{{ config (
    materialized = "view",
    tags = ['full_evm_test']
) }}

SELECT
    *
FROM
    {{ ref('core_evm__fact_event_logs') }}