{{ config (
    materialized = "view",
    tags = ['recent_evm_test']
) }}

SELECT
    *
FROM
    {{ ref('testnet__fact_evm_traces') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref('_evm_testnet_block_lookback') }}
    )
