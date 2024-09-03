-- depends_on: {{ ref('bronze_evm__receipts') }}
-- depends_on: {{ ref('bronze_evm__FR_receipts') }}
{{ config(
    materialized = 'incremental',
    unique_key = "evm_receipts_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', 'block_number'],
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
    VALUE :blockNumber :: INT AS blockNumber,
    VALUE :contractAddress :: STRING AS contract_address,
    VALUE :cumulativeGasUsed :: INT AS cumulative_gas_used,
    VALUE :effectiveGasPrice :: INT AS effective_gas_price_unadj,
    VALUE :from :: STRING AS from_address,
    VALUE :effectiveGasPrice :: INT / pow(
        10,
        9
    ) AS effective_gas_price_adj,
    ZEROIFNULL(
        VALUE :gasUsed :: INT
    ) AS gas_used,
    VALUE :logs :: ARRAY AS logs,
    VALUE :logsBloom :: STRING AS logs_bloom,
    VALUE :revertReason :: STRING AS revert_reason,
    VALUE :root :: STRING AS root,
    VALUE :status :: INT AS status,
    VALUE :status :: INT = 1 AS tx_succeeded,
    IFF(
        VALUE :status :: INT = 1,
        'SUCCESS',
        'FAIL'
    ) AS tx_status,
    VALUE :transactionHash :: STRING AS tx_hash,
    VALUE :transactionIndex :: INT AS tx_index,
    CASE
        WHEN block_number <> blockNumber THEN NULL
        ELSE VALUE :transactionIndex :: INT
    END AS POSITION,
    VALUE :type :: STRING AS receipt_type,
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
