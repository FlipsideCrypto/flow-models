{{ config (
    materialized = "view",
    tags = ['recent_evm_test']
) }}

SELECT
    *
FROM
    {{ ref('core_evm__fact_transactions') }}
WHERE
    block_number > (
        SELECT
            block_number
        FROM
            {{ ref('_evm_block_lookback') }}
    )
