{{ config(
    materialized = 'incremental',
    unique_key = "evm_txs_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_number'],
    tags = ['evm']
) }}

WITH tx_array AS (

    SELECT
        block_number,
        block_hash,
        block_timestamp,
        transactions,
        _partition_by_block_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_evm__blocks') }}
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
flatten_txs AS (
    SELECT
        block_number,
        block_hash,
        block_timestamp,
        INDEX AS array_index,
        VALUE :: variant AS tx_response,
        _partition_by_block_id,
        _inserted_timestamp
    FROM
        tx_array,
        LATERAL FLATTEN (transactions)
),
base_tx AS (
    SELECT
        block_number,
        block_hash,
        block_timestamp,
        utils.udf_hex_to_int(
            tx_response :blockNumber :: STRING
        ) AS blockNumber,
        utils.udf_hex_to_int(
            tx_response :chainId :: STRING
        ) AS chain_id,
        tx_response :from :: STRING AS from_address,
        utils.udf_hex_to_int(
            tx_response :gas :: STRING
        ) AS gas,
        utils.udf_hex_to_int(
            tx_response :gasPrice :: STRING
        ) AS gas_price_unadj,
        ZEROIFNULL(gas_price_unadj / pow(10, 9)) AS gas_price_adj,
        tx_response :hash :: STRING AS tx_hash,
        tx_response :input :: STRING AS input_data,
        SUBSTR(
            input_data,
            1,
            10
        ) AS origin_function_signature,
        -- note, no maxFeePerGas or maxPriorityFeePerGas
        utils.udf_hex_to_int(
            tx_response :nonce :: STRING
        ) AS nonce,
        utils.udf_hex_to_int(
            tx_response :r :: STRING
        ) AS r,
        utils.udf_hex_to_int(
            tx_response :s :: STRING
        ) AS s,
        -- note, no sourceHash
        tx_response :to :: STRING AS to_address,
        utils.udf_hex_to_int(
            tx_response :transactionIndex :: STRING
        ) AS POSITION,
        utils.udf_hex_to_int(
            tx_response :type :: STRING
        ) AS tx_type,
        utils.udf_hex_to_int(
            tx_response :v :: STRING
        ) AS v,
        utils.udf_hex_to_int(
            tx_response :value :: STRING
        ) AS value_precise_unadj,
        value_precise_unadj / pow(
            10,
            18
        ) AS value_precise_adj,
        value_precise_adj :: FLOAT AS VALUE,
        -- note, no yParity
        _partition_by_block_id,
        _inserted_timestamp
    FROM
        flatten_txs
),
new_records AS (
    SELECT
        t.block_number,
        t.block_hash,
        t.chain_id,
        t.from_address,
        t.gas,
        t.gas_price_unadj,
        t.gas_price_adj,
        t.tx_hash,
        t.input_data,
        t.origin_function_signature,
        t.nonce,
        t.r,
        t.s,
        t.to_address,
        t.position,
        t.tx_type,
        t.v,
        t.value_precise_unadj,
        t.value_precise_adj,
        t.value,
        t.block_timestamp,
        r.tx_status IS NULL AS is_pending,
        r.gas_used,
        r.gas_used * t.gas_price_unadj :: bigint / pow(
            10,
            18
        ) AS tx_fee_precise,
        ZEROIFNULL(
            tx_fee_precise :: FLOAT
        ) AS tx_fee,
        r.tx_succeeded,
        r.tx_status,
        r.cumulative_gas_used,
        r.effective_gas_price_adj AS effective_gas_price,
        r.receipt_type,
        t._inserted_timestamp,
        t._partition_by_block_id
    FROM
        base_tx t
        LEFT OUTER JOIN {{ ref('silver_evm__receipts') }}
        r
        ON t.block_number = r.block_number
        AND t.tx_hash = r.tx_hash

{% if is_incremental() %}
AND r._INSERTED_TIMESTAMP >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 1
    FROM
        {{ this }}
)
{% endif %}
),

{% if is_incremental() %}
missing_data AS (
    SELECT
        t.block_number,
        t.block_hash,
        t.chain_id,
        t.from_address,
        t.gas,
        t.gas_price_adj,
        t.gas_price_unadj,
        t.tx_hash,
        t.input_data,
        t.origin_function_signature,
        t.nonce,
        t.r,
        t.s,
        t.to_address,
        t.position,
        t.tx_type,
        t.v,
        t.value_precise_unadj,
        t.value_precise_adj,
        t.value,
        t.block_timestamp,
        FALSE AS is_pending,
        r.gas_used,
        r.tx_succeeded,
        r.tx_status,
        r.cumulative_gas_used,
        r.effective_gas_price_adj AS effective_gas_price,
        r.gas_used * t.gas_price_unadj :: bigint / pow(
            10,
            18
        ) AS tx_fee_precise_heal,
        ZEROIFNULL(
            tx_fee_precise_heal :: FLOAT
        ) AS tx_fee,
        r.receipt_type,
        GREATEST(
            t._inserted_timestamp,
            r._inserted_timestamp
        ) AS _inserted_timestamp,
        t._partition_by_block_id
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver_evm__receipts') }}
        r
        ON t.tx_hash = r.tx_hash
        AND t.block_number = r.block_number
    WHERE
        t.is_pending
),
{% endif %}

FINAL AS (
    SELECT
        block_number,
        block_hash,
        chain_id,
        from_address,
        gas,
        gas_price_adj,
        gas_price_unadj,
        tx_hash,
        input_data,
        origin_function_signature,
        nonce,
        r,
        s,
        to_address,
        POSITION,
        tx_type,
        v,
        VALUE,
        value_precise_unadj,
        value_precise_adj,
        block_timestamp,
        is_pending,
        gas_used,
        tx_succeeded,
        tx_status,
        cumulative_gas_used,
        effective_gas_price,
        tx_fee,
        tx_fee_precise,
        receipt_type,
        _inserted_timestamp,
        _partition_by_block_id
    FROM
        new_records

{% if is_incremental() %}
UNION
SELECT
    block_number,
    block_hash,
    chain_id,
    from_address,
    gas,
    gas_price_adj,
    gas_price_unadj,
    tx_hash,
    input_data,
    origin_function_signature,
    nonce,
    r,
    s,
    to_address,
    POSITION,
    tx_type,
    v,
    VALUE,
    value_precise_unadj,
    value_precise_adj,
    block_timestamp,
    is_pending,
    gas_used,
    tx_succeeded,
    tx_status,
    cumulative_gas_used,
    effective_gas_price,
    tx_fee,
    tx_fee_precise_heal AS tx_fee_precise,
    receipt_type,
    _inserted_timestamp,
    _partition_by_block_id
FROM
    missing_data
{% endif %}
)
SELECT
    block_number,
    block_hash,
    chain_id,
    from_address,
    gas,
    gas_price_adj,
    gas_price_unadj,
    tx_hash,
    input_data,
    origin_function_signature,
    nonce,
    r,
    s,
    to_address,
    POSITION,
    tx_type AS TYPE,
    v,
    VALUE,
    value_precise_unadj,
    value_precise_adj,
    block_timestamp,
    is_pending,
    gas_used,
    tx_succeeded,
    tx_status,
    cumulative_gas_used,
    effective_gas_price,
    tx_fee,
    tx_fee_precise,
    receipt_type AS tx_type,
    _inserted_timestamp,
    _partition_by_block_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS evm_txs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
