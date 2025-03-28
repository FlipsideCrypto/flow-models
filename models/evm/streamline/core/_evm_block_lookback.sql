{{ config (
    materialized = "ephemeral"
) }}

SELECT
    MIN(block_number) AS block_number
FROM
    {{ ref("core_evm__fact_blocks") }}
WHERE
    block_timestamp >= DATEADD('hour', -72, TRUNCATE(SYSDATE(), 'HOUR'))
    AND block_timestamp < DATEADD('hour', -71, TRUNCATE(SYSDATE(), 'HOUR'))
