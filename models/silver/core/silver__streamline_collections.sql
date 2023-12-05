-- depends_on: {{ ref('bronze__streamline_collections') }}
{{ config(
    materialized = 'incremental',
    unique_key = "collection_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', 'block_number'],
    tags = ['streamline_load', 'core', 'scheduled_core']
) }}

SELECT
    block_number,
    DATA: id :: STRING AS collection_id,
    ARRAY_SIZE(
        DATA :transaction_ids :: ARRAY
    ) AS tx_count,
    DATA: transaction_ids :: ARRAY AS transaction_ids,
    _partition_by_block_id,
    {{ dbt_utils.generate_surrogate_key(
            ['collection_id']
        ) }} AS streamline_collection_id,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_collections') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_collections') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY collection_id
ORDER BY
    _inserted_timestamp DESC)) = 1
