-- depends_on: {{ ref('bronze_evm__blocks') }}
-- depends_on: {{ ref('bronze_evm__FR_blocks') }}
{{ config(
    materialized = 'incremental',
    unique_key = "evm_blocks_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', '_partition_by_block_id'],
    tags = ['evm']
) }}

SELECT
    block_number,
    DATA :result :hash :: STRING AS block_hash,
    TO_TIMESTAMP(
        utils.udf_hex_to_int(
            DATA :result :timestamp :: STRING
        )
    ) AS block_timestamp,
    ARRAY_SIZE(
        DATA :result :transactions :: ARRAY
    ) AS transaction_count,
    utils.udf_hex_to_int(
        DATA :result :baseFeePerGas :: STRING
    ) AS base_fee_per_gas,
    utils.udf_hex_to_int(
        DATA :result :difficulty :: STRING
    ) AS difficulty,
    DATA :result :extraData :: STRING AS extra_data,
    utils.udf_hex_to_int(
        DATA :result :gasLimit :: STRING
    ) AS gas_limit,
    utils.udf_hex_to_int(
        DATA :result :gasUsed :: STRING
    ) AS gas_used,
    DATA :result :logsBloom :: STRING AS logs_bloom,
    DATA :result :miner :: STRING AS miner,
    DATA :result :mixHash :: STRING AS mix_hash,
    utils.udf_hex_to_int(
        DATA :result :nonce :: STRING
    ) AS nonce,
    DATA :result :parentHash :: STRING AS parent_hash,
    DATA :result :receiptsRoot :: STRING AS receipts_root,
    DATA :result :sha3Uncles :: STRING AS sha3_uncles,
    utils.udf_hex_to_int(
        DATA :result :size :: STRING
    ) AS SIZE,
    DATA :result :stateRoot :: STRING AS state_root,
    ZEROIFNULL(
        utils.udf_hex_to_int(
            DATA :result :totalDifficulty :: STRING
        )
    ) AS total_difficulty,
    DATA :result :transactions :: ARRAY AS transactions,
    DATA :result :transactionsRoot :: STRING AS transactions_root,
    DATA :result :uncles :: ARRAY AS uncles,
    partition_key AS _partition_by_block_id,
    {{ dbt_utils.generate_surrogate_key(
        ['data:result:hash::STRING']
    ) }} AS evm_blocks_id,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze_evm__blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze_evm__FR_blocks') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY evm_blocks_id
ORDER BY
    _inserted_timestamp DESC)) = 1
