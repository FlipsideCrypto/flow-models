{{ config(
    materialized = 'view',
    tags = ['ez', 'scheduled']
) }}

WITH chainwalkers AS (

    SELECT
        NULL AS token_transfers_id,
        block_height,
        block_timestamp,
        tx_id,
        sender,
        recipient,
        token_contract,
        amount,
        tx_succeeded,
        _inserted_timestamp,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
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
        token_transfers_id,
        block_height,
        block_timestamp,
        tx_id,
        sender,
        recipient,
        token_contract,
        amount,
        tx_succeeded,
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp
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
),
FINAL AS (
    SELECT
        *
    FROM
        streamline
    UNION ALL
    SELECT
        *
    FROM
        chainwalkers
)
SELECT
    block_height,
    block_timestamp,
    tx_id,
    sender,
    recipient,
    token_contract,
    amount,
    tx_succeeded,
    COALESCE (
        token_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','sender', 'recipient','token_contract', 'amount']
        ) }}
    ) AS ez_token_transfers_id,
    COALESCE (
        inserted_timestamp,
        _inserted_timestamp
    ) AS inserted_timestamp,
    COALESCE (
        modified_timestamp,
        _inserted_timestamp
    ) AS modified_timestamp
FROM
    FINAL