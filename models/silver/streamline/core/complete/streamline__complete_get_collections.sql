{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)"
) }}

SELECT
    id,
    data,
    block_number,
    _partition_by_block_id,
    _inserted_timestamp
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
    {{ ref('bronze__streamline_collections') }} -- TODO: change to bronze__streamline_FR_collections
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY _partition_by_block_id
ORDER BY
    _inserted_timestamp DESC)) = 1