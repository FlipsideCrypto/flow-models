{{ config(
    materialized = 'view',
    tags = ['scheduled']
) }}

WITH silver_txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}
),
gold_txs AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        chain_id,
        tx_index,
        proposer,
        payer,
        authorizers,
        count_authorizers,
        gas_limit,
        transaction_result,
        tx_succeeded,
        error_msg
    FROM
        silver_txs
)
SELECT
    *
FROM
    gold_txs