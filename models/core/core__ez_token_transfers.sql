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
    block_timestamp :: DATE >= '2022-04-20'
    AND (
        token_contract = 'A.c38aea683c0c4d38.ZelosAccountingToken'
        AND recipient IS NOT NULL
    )
