-- depends_on: {{ ref('bronze__streamline_transaction_results_history') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)",
    tags = ['streamline_history']
) }}

SELECT
    id,
    DATA,
    block_number,
    _partition_by_block_id,
    _inserted_timestamp
FROM

{{ ref('bronze__streamline_transaction_results_history') }}
WHERE
    TRUE
{% if is_incremental() %}
    AND _inserted_timestamp >= COALESCE(
        (
            SELECT
                MAX(_inserted_timestamp) _inserted_timestamp
            FROM
                {{ this }}
        ),
        '1900-01-01' :: timestamp_ntz
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
