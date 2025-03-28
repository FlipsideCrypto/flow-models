-- depends_on: {{ ref('bronze__streamline_transaction_results') }}
{{ config (
    materialized = "incremental",
    unique_key = "id",
    cluster_by = "ROUND(block_number, -3)",
    merge_update_columns = ["id"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(id)",
    tags = ['streamline_complete']
) }}

SELECT
    id,
    DATA,
    block_number,
    _partition_by_block_id,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transaction_results') }}
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
    AND _partition_by_block_id > 108000000
    -- id NOT IN (
    --     'f31f601728b59a0411b104e6795eb18e32c9b1bea3e52ea1d28a801ed5ceb009',
    --     'b68b81b7a2ec9fb4e3789f871f95084ba4fdd9b46bb6c7029efa578a69dba432'
    -- )
{% else %}
    {{ ref('bronze__streamline_fr_transaction_results') }}
WHERE
    TRUE
{% endif %}
AND NOT (
    DATA :status :: INT < 4
    AND block_number >= (
        SELECT
            MAX(root_height)
        FROM
            {{ ref('seeds__network_version') }}
    )
) 

qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1
