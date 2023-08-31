-- depends_on: {{ ref('bronze__streamline_blocks') }}
-- depends_on: {{ ref('bronze__streamline_fr_blocks') }}
{{ config (
    materialized = "incremental",
    unique_key = "block_number",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    tags = ['streamline_complete']
) }}

SELECT
    DATA,
    block_number,
    _partition_by_block_id,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks') }}
WHERE
    _inserted_timestamp >= COALESCE(
        (
            SELECT
                MAX(_inserted_timestamp) _inserted_timestamp
            FROM
                {{ this }}
        ),
        '1900-01-01' :: timestamp_ntz
    )
{% else %}
    {{ ref('bronze__streamline_fr_blocks') }}
WHERE
    TRUE
{% endif %}
AND NOT (
    DATA :status :: INT != 2
    AND block_number >= 55114467
) 
qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
