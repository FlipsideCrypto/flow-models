-- depends_on: {{ ref('bronze__streamline_testnet_transaction_results') }}
-- depends_on: {{ ref('bronze__streamline_fr_testnet_transaction_results') }}
{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "_partition_by_block_id"],
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ["block_number","_inserted_timestamp::date"],
    tags = ['testnet']
) }}

SELECT
    block_number,
    id AS tx_id,
    DATA :error_message :: STRING AS error_message,
    DATA :events :: ARRAY AS events,
    DATA :status :: INT AS status,
    DATA :status_code :: INT AS status_code,
    _partition_by_block_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }} AS tx_results_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id  
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_testnet_transaction_results') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )

{% else %}
    {{ ref('bronze__streamline_fr_testnet_transaction_results') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1
