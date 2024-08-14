{{ config(
    materialized = 'incremental',
    unique_key = "evm_testnet_txs_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_number'],
    tags = ['evm_testnet']
) }}

WITH tx_array AS (

    SELECT
        block_number,
        block_hash,
        block_timestamp,
        {# block_response, #}
        block_response :transactions :: ARRAY AS transactions,
        _partition_by_block_id
    FROM
        {{ ref('silver__evm_testnet_blocks') }}
    WHERE
        transaction_count > 0

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) modified_timestamp
    FROM
        {{ this }}
)
{% endif %}
),
flattened_txs AS (
    SELECT
        block_number,
        block_hash,
        block_timestamp,
        VALUE :: VARIANT AS tx_response,
        _partition_by_block_id
    FROM
        tx_array,
        LATERAL FLATTEN (transactions)
)
SELECT
    block_number,
    block_hash,
    block_timestamp,
    tx_response :chainId :: STRING as chain_id_hex,
    tx_response :from :: STRING as from_address,
    tx_response :gas :: STRING as gas_hex,
    tx_response :gasPrice :: STRING as gas_price_hex,
    tx_response :hash :: STRING as tx_hash,
    tx_response :input :: STRING as input_data,
    tx_response :nonce :: STRING as nonce_hex,
    tx_response :r :: STRING as r_hex,
    tx_response :s :: STRING as s_hex,
    tx_response :to :: STRING as to_address,
    tx_response :transactionIndex :: STRING as transaction_index_hex,
    tx_response :type :: STRING as tx_type_hex,
    tx_response :v :: STRING as v_hex,
    tx_response :value :: STRING as value_hex,
    _partition_by_block_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flattened_txs
