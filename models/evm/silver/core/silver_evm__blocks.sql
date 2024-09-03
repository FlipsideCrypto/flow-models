-- depends_on: {{ ref('bronze_evm__blocks') }}
-- depends_on: {{ ref('bronze_evm__FR_blocks') }}
{{ config(
    materialized = 'incremental',
    unique_key = "evm_blocks_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', 'block_number'],
    tags = ['evm']
) }}

SELECT
    block_number,
    DATA :result :hash :: STRING AS block_hash,
    TO_TIMESTAMP(
        livequery.utils.udf_hex_to_int(
            DATA :result :timestamp :: STRING
        )
    ) AS block_timestamp,
    ARRAY_SIZE(
        DATA :result :transactions :: ARRAY
    ) AS transaction_count,
    DATA :result :: variant AS block_response,
    VALUE,
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
