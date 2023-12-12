{{ config(
    materialized = 'view',
    tags = ['scheduled']
) }}

WITH chainwalkers AS (

    SELECT
        NULL AS streamline_transaction_id,
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
        error_msg,
        _inserted_timestamp,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_height < {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
streamline AS (
    SELECT
        streamline_transaction_id,
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
        error_message AS error_msg,
        _inserted_timestamp,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        NOT pending_result_response
        AND block_height >= {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
FINAL AS (
    SELECT
        *
    FROM
        chainwalkers
    UNION ALL
    SELECT
        *
    FROM
        streamline
)
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
    error_msg,
    COALESCE (
        streamline_transaction_id,
        {{ dbt_utils.generate_surrogate_key(['tx_id']) }}
    ) AS fact_transactions_id,
    COALESCE (
        inserted_timestamp,
        _inserted_timestamp
    ) AS inserted_timestamp,
    COALESCE (
        modified_timestamp,
        _inserted_timestamp
    ) AS modified_timestamp
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY streamline_transaction_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
