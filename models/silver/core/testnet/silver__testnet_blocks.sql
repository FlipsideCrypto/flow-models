-- depends_on: {{ ref('bronze__streamline_testnet_blocks') }}
-- depends_on: {{ ref('bronze__streamline_fr_testnet_blocks') }}
{{ config(
    materialized = 'incremental',
    unique_key = "testnet_blocks_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', 'block_number'],
    tags = ['testnet', 'crescendo']
) }}

SELECT
    block_number,
    DATA :height :: INT AS block_height,
    DATA :id :: STRING AS id,
    DATA :timestamp :: timestamp_ntz AS block_timestamp,
    ARRAY_SIZE(
        DATA :collection_guarantees :: ARRAY
    ) AS collection_count,
    DATA: parent_id :: STRING AS parent_id,
    DATA,
    _partition_by_block_id,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS testnet_blocks_id,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_testnet_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_testnet_blocks') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY testnet_blocks_id
ORDER BY
    _inserted_timestamp DESC)) = 1
