{{ config (
    materialized = "incremental",
    unique_key = "record_id",
    cluster_by = "ROUND(block_id, -3)",
    merge_update_columns = ["record_id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(record_id)"
) }}

SELECT
    record_id,
    block_id,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__blocks') }} -- TODO: change to bronze__streamline_blocks
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )

{% else %}
    {{ ref('bronze__blocks') }} -- TODO: change to bronze__streamline_FR_blocks
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY record_id
ORDER BY
    _inserted_timestamp DESC)) = 1