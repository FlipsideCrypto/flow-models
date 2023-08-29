-- depends_on: {{ ref('bronze__streamline_collections') }}
{{ config(
    materialized = 'incremental',
    unique_key = "collections_id",
    cluster_by = "block_number",
    tags = ['core']
) }}

SELECT
    block_number,
    DATA: id :: STRING AS collections_id,
    ARRAY_SIZE(
        DATA :transaction_ids :: ARRAY
    ) AS transactions_count,
    DATA: transaction_ids :: ARRAY AS transaction_ids,
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
    {{ ref('bronze__streamline_fr_collections') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
