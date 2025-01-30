{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['evm']
) }}

WITH base AS (

    SELECT
        block_number,
        {% if uses_receipts_by_hash %}
            tx_hash,
        {% else %}
            receipts_json :transactionHash :: STRING AS tx_hash,
        {% endif %}
        receipts_json,
        receipts_json :logs AS full_logs
    FROM
        {{ ref('silver_evm__receipts') }}
    WHERE
        1 = 1
        AND ARRAY_SIZE(receipts_json :logs) > 0

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
    FROM
        {{ this }})
    {% endif %}
),
flattened_logs AS (
    SELECT
        block_number,
        tx_hash,
        lower(receipts_json :from :: STRING) AS origin_from_address,
        lower(receipts_json :to :: STRING) AS origin_to_address,
        CASE
            WHEN receipts_json :status :: STRING = '0x1' THEN TRUE
            WHEN receipts_json :status :: STRING = '1' THEN TRUE
            WHEN receipts_json :status :: STRING = '0x0' THEN FALSE
            WHEN receipts_json :status :: STRING = '0' THEN FALSE
            ELSE NULL
        END AS tx_succeeded,
        VALUE :address :: STRING AS contract_address,
        VALUE :blockHash :: STRING AS block_hash,
        VALUE :blockNumber :: STRING AS block_number_hex,
        VALUE :data :: STRING AS DATA,
        utils.udf_hex_to_int(
            VALUE :logIndex :: STRING
        ) :: INT AS event_index,
        VALUE :removed :: BOOLEAN AS event_removed,
        VALUE :topics AS topics,
        VALUE :transactionHash :: STRING AS transaction_hash,
        utils.udf_hex_to_int(
            VALUE :transactionIndex :: STRING
        ) :: INT AS transaction_index
    FROM
        base,
        LATERAL FLATTEN (
            input => full_logs
        )
),
new_logs AS (
    SELECT
        l.block_number,
        b.block_timestamp,
        l.tx_hash,
        l.transaction_index AS tx_position,
        l.event_index,
        l.contract_address,
        l.topics,
        l.topics [0] :: STRING AS topic_0,
        l.topics [1] :: STRING AS topic_1,
        l.topics [2] :: STRING AS topic_2,
        l.topics [3] :: STRING AS topic_3,
        l.data,
        l.event_removed,
        txs.from_address AS origin_from_address,
        txs.to_address AS origin_to_address,
        txs.origin_function_signature,
        l.tx_succeeded
    FROM
        flattened_logs l
        LEFT JOIN {{ ref('core_evm__fact_blocks') }}
        b
        ON l.block_number = b.block_number

{% if is_incremental() %}
AND b.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
LEFT JOIN {{ ref('core_evm__fact_transactions') }}
txs
ON l.tx_hash = txs.tx_hash
AND l.block_number = txs.block_number

{% if is_incremental() %}
AND txs.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.block_number,
        b.block_timestamp AS block_timestamp_heal,
        t.tx_hash,
        t.tx_position,
        t.event_index,
        t.contract_address,
        t.topics,
        t.topic_0,
        t.topic_1,
        t.topic_2,
        t.topic_3,
        t.data,
        t.event_removed,
        txs.from_address AS origin_from_address_heal,
        txs.to_address AS origin_to_address_heal,
        txs.origin_function_signature AS origin_function_signature_heal,
        t.tx_succeeded
    FROM
        {{ this }}
        t
        LEFT JOIN {{ ref('core_evm__fact_transactions') }}
        txs
        ON t.tx_hash = txs.tx_hash
        AND t.block_number = txs.block_number
        LEFT JOIN {{ ref('core_evm__fact_blocks') }}
        b
        ON t.block_number = b.block_number
    WHERE
        t.block_timestamp IS NULL
        OR t.origin_function_signature IS NULL
)
{% endif %},
all_logs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        contract_address,
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA,
        event_removed,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_succeeded
    FROM
        new_logs

{% if is_incremental() %}
UNION ALL
SELECT
    block_number,
    block_timestamp_heal AS block_timestamp,
    tx_hash,
    tx_position,
    event_index,
    contract_address,
    topics,
    topic_0,
    topic_1,
    topic_2,
    topic_3,
    DATA,
    event_removed,
    origin_from_address_heal AS origin_from_address,
    origin_to_address_heal AS origin_to_address,
    origin_function_signature_heal AS origin_function_signature,
    tx_succeeded
FROM
    missing_data
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    event_index,
    contract_address,
    topics,
    topic_0,
    topic_1,
    topic_2,
    topic_3,
    DATA,
    event_removed,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_succeeded,
    {{ dbt_utils.generate_surrogate_key(['tx_hash','event_index']) }} AS fact_event_logs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    all_logs qualify ROW_NUMBER() over (
        PARTITION BY fact_event_logs_id
        ORDER BY
            block_number DESC,
            block_timestamp DESC nulls last,
            origin_function_signature DESC nulls last
    ) = 1