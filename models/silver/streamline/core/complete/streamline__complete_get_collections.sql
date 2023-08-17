-- depends_on: {{ ref('bronze__streamline_collections') }}
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
   _inserted_timestamp >= COALESCE(
        (
            SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
            FROM
                {{ this }}
        ),
        '1900-01-01'::timestamp
    )
{% else %}

    {{ ref('bronze__streamline_fr_collections') }} 
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY _partition_by_block_id
ORDER BY
    _inserted_timestamp DESC)) = 1