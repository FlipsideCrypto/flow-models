-- depends_on: {{ ref('bronze__evm_traces') }}
-- depends_on: {{ ref('bronze__fr_evm_traces') }}
{{ config(
    materialized = 'incremental',
    unique_key = "evm_traces_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', 'block_number'],
    tags = ['evm']
) }}

WITH traces AS (

    SELECT
        block_number,
        DATA,
        _partition_by_block_id,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__evm_traces') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__fr_evm_traces') }}
{% endif %}
qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
),
flatten_traces AS (
SELECT
    block_number,
    INDEX AS array_index,
    VALUE :: VARIANT AS trace_response,
    _partition_by_block_id,
    _inserted_timestamp
FROM
    traces,
    LATERAL FLATTEN (DATA :result :: variant) 

)
SELECT
    block_number,
    array_index,
    trace_response :from :: STRING AS from_address,
    trace_response :gas :: STRING AS gas_hex,
    trace_response :gasUsed :: STRING AS gas_used_hex,
    trace_response :input :: STRING AS input,
    trace_response :to :: STRING AS to_address,
    trace_response :type :: STRING AS trace_type,
    trace_response :value :: STRING AS value_hex,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'array_index']
    ) }} AS evm_traces_id,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_traces
