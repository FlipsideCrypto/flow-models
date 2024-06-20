{{ config(
    materialized = 'view',
    tags = ['scheduled']
) }}

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
    script,
    arguments,
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
    COALESCE (
        streamline_transaction_id,
        {{ dbt_utils.generate_surrogate_key(['tx_id']) }}
    ) AS fact_transactions_id,
    _inserted_timestamp,
    NULL AS inserted_timestamp,
    NULL AS modified_timestamp
FROM
    {{ ref('silver__streamline_transactions_final') }}
WHERE
    NOT pending_result_response
