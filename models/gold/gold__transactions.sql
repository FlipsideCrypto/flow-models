{{ config(
    materialized = 'incremental',
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'tx_id',
    incremental_strategy = 'delete+insert'
) }}

WITH silver_txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}

{% if is_incremental() %}
WHERE
    _ingested_at :: DATE > CURRENT_DATE - 2
{% endif %}
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
