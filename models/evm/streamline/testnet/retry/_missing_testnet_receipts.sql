{{ config (
    materialized = "ephemeral"
) }}

SELECT
    DISTINCT block_number AS block_number
FROM
    {{ ref("testnet_evm__fact_transactions") }}
WHERE
    tx_succeeded IS NULL
    AND block_number > (
        SELECT
            block_number
        FROM
             {{ ref("_evm_testnet_block_lookback") }}
    )