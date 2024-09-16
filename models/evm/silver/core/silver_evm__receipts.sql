-- depends_on: {{ ref('bronze_evm__receipts') }}
-- depends_on: {{ ref('bronze_evm__FR_receipts') }}
{{ config(
    materialized = 'incremental',
    unique_key = "evm_receipts_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', '_partition_by_block_id'],
    tags = ['evm']
) }}

WITH receipts AS (

    SELECT
        block_number,
        DATA,
        partition_key AS _partition_by_block_id,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze_evm__receipts') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze_evm__FR_receipts') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
),
FINAL AS (
    SELECT
        block_number,
        COALESCE(
            VALUE :PrecompiledCalls :: STRING,
            VALUE :precompiledCalls :: STRING
        ) AS precompiled_calls,
        VALUE :blobGasPrice :: INT AS blob_gas_price,
        VALUE :blockHash :: STRING AS block_hash,
        IFF(LEFT(VALUE :blockNumber :: STRING, 2) = '0x', utils.udf_hex_to_int(VALUE :blockNumber :: STRING), VALUE :blockNumber) :: INT AS blockNumber,
        VALUE :contractAddress :: STRING AS contract_address,
        IFF(LEFT(VALUE :cumulativeGasUsed :: STRING, 2) = '0x', utils.udf_hex_to_int(VALUE :cumulativeGasUsed :: STRING), VALUE :cumulativeGasUsed) :: INT AS cumulative_gas_used,
        IFF(LEFT(VALUE :effectiveGasPrice :: STRING, 2) = '0x', utils.udf_hex_to_int(VALUE :effectiveGasPrice :: STRING), VALUE :effectiveGasPrice) :: INT AS effective_gas_price_unadj,
        VALUE :from :: STRING AS from_address,
        effective_gas_price_unadj / pow(
            10,
            9
        ) AS effective_gas_price_adj,
        ZEROIFNULL(
            IFF(LEFT(VALUE :gasUsed :: STRING, 2) = '0x', utils.udf_hex_to_int(VALUE :gasUsed :: STRING), VALUE :gasUsed) :: INT
        ) AS gas_used,
        VALUE :logs :: ARRAY AS logs,
        VALUE :logsBloom :: STRING AS logs_bloom,
        VALUE :revertReason :: STRING AS revert_reason,
        VALUE :root :: STRING AS root,
        IFF(LEFT(VALUE :status :: STRING, 2) = '0x', utils.udf_hex_to_int(VALUE :status :: STRING), VALUE :status) :: INT AS status,
        status = 1 AS tx_succeeded,
        IFF(
            tx_succeeded,
            'SUCCESS',
            'FAIL'
        ) AS tx_status,
        VALUE :transactionHash :: STRING AS tx_hash,
        IFF(
            LEFT(
                VALUE :transactionIndex :: STRING,
                2
            ) = '0x',
            utils.udf_hex_to_int(
                VALUE :transactionIndex :: STRING
            ),
            VALUE :transactionIndex
        ) :: INT AS tx_index,
        CASE
            WHEN block_number <> blockNumber THEN NULL
            ELSE tx_index
        END AS POSITION,
        IFF(LEFT(VALUE :type :: STRING, 2) = '0x', utils.udf_hex_to_int(VALUE :type :: STRING), VALUE :type) :: INT AS receipt_type,
        VALUE :to :: STRING AS to_address,
        _partition_by_block_id,
        _inserted_timestamp
    FROM
        receipts,
        LATERAL FLATTEN (
            DATA :result :: variant
        )
)
SELECT
    block_number,
    precompiled_calls,
    blob_gas_price,
    block_hash,
    blockNumber,
    contract_address,
    cumulative_gas_used,
    effective_gas_price_unadj,
    effective_gas_price_adj,
    from_address,
    gas_used,
    logs,
    logs_bloom,
    revert_reason,
    root,
    status,
    tx_succeeded,
    tx_status,
    tx_hash,
    tx_index,
    POSITION,
    receipt_type,
    to_address,
    _partition_by_block_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'tx_hash']
    ) }} AS evm_receipts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
WHERE
    tx_hash IS NOT NULL
    AND POSITION IS NOT NULL
