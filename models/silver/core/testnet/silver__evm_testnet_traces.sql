-- depends_on: {{ ref('bronze__streamline_evm_testnet_traces') }}
-- depends_on: {{ ref('bronze__streamline_fr_evm_testnet_traces') }}
{{ config(
    materialized = 'incremental',
    unique_key = "evm_testnet_traces_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', 'block_number'],
    tags = ['evm_testnet', 'crescendo']
) }}

WITH traces AS (

    SELECT
        block_number,
        DATA,
        _partition_by_block_id,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_evm_testnet_traces') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_evm_testnet_traces') }}
{% endif %}
)
SELECT
    block_number,
    INDEX AS trace_index,
    VALUE,
    VALUE :from :: STRING AS from_address,
    VALUE :gas :: STRING AS gas_hex,
    VALUE :gasUsed :: STRING AS gas_used_hex,
    VALUE :input :: STRING AS input,
    VALUE :to :: STRING AS to_address,
    VALUE :type :: STRING AS trace_type,
    VALUE :value :: STRING AS value_hex,
    _partition_by_block_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'INDEX']
    ) }} AS evm_testnet_traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    traces,
    LATERAL FLATTEN (DATA :result :: variant) 

qualify(ROW_NUMBER() over (PARTITION BY evm_testnet_traces_id
ORDER BY
    _inserted_timestamp DESC)) = 1
