-- depends_on: {{ ref('bronze__streamline_blocks') }}
{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date",
    tags = ['core']
) }}

SELECT
    block_number,
    DATA : height :: STRING AS block_height,
    DATA : id :: STRING AS block_id,
    DATA :timestamp :: TIMESTAMP AS block_timestamp,
    ARRAY_SIZE(
        DATA :collection_guarantees
    ) AS collection_count,
    DATA : parent_id :: STRING AS parent_id,
    DATA : signatures AS signatures,
    DATA : collection_guarantees AS collection_guarantees,
    DATA :  block_seals AS block_seals,
    _partition_by_block_id,
    _inserted_timestamp
FROM
{% if is_incremental() %}
{{ ref('bronze__streamline_blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_blocks') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1