{{ config(
    materialized = 'view'
) }}


SELECT
    block_height,
    block_timestamp,
    tx_id,
    sender,
    recipient,
    token_contract,
    amount,
    tx_succeeded
FROM
     {{ ref('silver__token_transfers') }}
WHERE 
    block_timestamp::date >= '2022-04-20'