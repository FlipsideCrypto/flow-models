{{ config(
    materialized = 'view',
    tags = ['scheduled']
) }}

WITH chainwalkers AS (

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
        {{ ref('silver__transactions') }}
    WHERE
        block_height < {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
streamline AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        'flow' AS chain_id,
        NULL AS tx_index,
        proposer,
        payer,
        authorizers,
        count_authorizers,
        gas_limit,
        OBJECT_CONSTRUCT(
            'error',
            error_message,
            'events',
            events,
            'status',
            status
        ) AS transaction_result,
        tx_succeeded,
        error_message AS error_msg
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        NOT pending_result_response
        AND block_height >= {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
)
SELECT
    *
FROM
    chainwalkers
UNION ALL
SELECT
    *
FROM
    streamline
