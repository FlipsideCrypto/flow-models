{{ config(
    materialized = 'view',
    tags = ['ez', 'scheduled']
) }}

WITH chainwalkers AS (

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
        AND block_height < {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
streamline AS (
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
        {{ ref('silver__token_transfers_s') }}
    WHERE
        token_contract NOT IN (
            'A.c38aea683c0c4d38.ZelosAccountingToken',
            'A.f1b97c06745f37ad.SwapPair'
        )
        AND block_height >= {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
)
SELECT
    *
FROM
    streamline
UNION
SELECT
    *
FROM
    chainwalkers
