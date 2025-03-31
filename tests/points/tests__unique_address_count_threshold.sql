{{ config(
    severity = "error",
    tags = ["streamline_non_core"]
) }}
{# This test is to alert if the total EVM Addresses increases and the 
model calling balances needs to be adjusted with a higher SQL Limit #}
WITH distinct_count AS (

    SELECT
        COUNT(
            DISTINCT address
        ) AS ct
    FROM
        {{ ref('streamline__evm_addresses') }}
)
SELECT
    *
FROM
    distinct_count
WHERE
    ct > 90000
