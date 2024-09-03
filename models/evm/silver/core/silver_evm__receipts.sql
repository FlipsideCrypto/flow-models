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

)
SELECT
    block_number,
    VALUE :blobGasPrice :: INT AS blob_gas_price,
    VALUE :blockHash :: STRING AS block_hash,
    VALUE :contractAddress :: STRING AS contract_address,
    VALUE :cumulativeGasUsed :: INT AS cumulative_gas_used,
    VALUE :effectiveGasPrice :: INT AS effective_gas_price,
    VALUE :logs :: ARRAY AS logs,
    VALUE :logsBloom :: STRING AS logs_bloom,
    VALUE :revertReason :: STRING AS revert_reason,
    VALUE :status :: INT AS tx_status,
    VALUE :transactionHash :: STRING AS tx_hash,
    VALUE :transactionIndex :: INT AS tx_index,
    VALUE :type :: STRING AS tx_type,
    _partition_by_block_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'VALUE:transactionHash::STRING']
    ) }} AS evm_receipts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    receipts,
    LATERAL FLATTEN (DATA :result :: variant)
