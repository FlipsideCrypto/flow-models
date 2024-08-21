-- depends_on: {{ ref('bronze__streamline_testnet_collections') }}
-- depends_on: {{ ref('bronze__streamline_fr_testnet_collections') }}
{{ config(
    materialized = 'incremental',
    unique_key = "testnet_collections_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', 'block_number'],
    tags = ['testnet', 'crescendo']
) }}

SELECT
    block_number,
    DATA :id :: STRING AS collection_id,
    ARRAY_SIZE(
        DATA :transaction_ids :: ARRAY
    ) AS tx_count,
    DATA: transaction_ids :: ARRAY AS transaction_ids,
    DATA,
    _partition_by_block_id,
    {{ dbt_utils.generate_surrogate_key(
        ['DATA :id :: STRING']
    ) }} AS testnet_collections_id,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_testnet_collections') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_testnet_collections') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY testnet_collections_id
ORDER BY
    _inserted_timestamp DESC)) = 1
