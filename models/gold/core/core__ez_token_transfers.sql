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
WHERE
    token_contract NOT IN (
        'A.c38aea683c0c4d38.ZelosAccountingToken',
        'A.f1b97c06745f37ad.SwapPair'
    )
