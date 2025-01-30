{{ config (
    materialized = "view",
    tags = ['recent_evm_test']
) }}

SELECT
    *
FROM
    {{ ref('silver_evm__contracts') }}
WHERE
    inserted_timestamp > DATEADD(DAY, -5, SYSDATE())
