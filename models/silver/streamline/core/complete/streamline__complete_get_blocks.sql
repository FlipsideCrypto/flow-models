-- depends_on: {{ ref('bronze__streamline_blocks') }}
-- depends_on: {{ ref('bronze__streamline_FR_blocks') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)"
) }}

SELECT
    data,
    block_number,
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
    {{ ref('bronze__streamline_FR_blocks') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1