{{ config(
    materialized = 'view',
    tags = ['ez']
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
