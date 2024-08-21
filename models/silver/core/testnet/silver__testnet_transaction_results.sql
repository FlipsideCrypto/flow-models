-- depends_on: {{ ref('bronze__streamline_testnet_transaction_results') }}
-- depends_on: {{ ref('bronze__streamline_fr_testnet_transaction_results') }}
{{ config(
    materialized = 'incremental',
    unique_key = "testnet_transaction_results_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE', 'block_number'],
    tags = ['testnet', 'crescendo']
) }}

SELECT
    block_number,
    VALUE :id :: STRING AS tx_id,
    DATA :error_message :: STRING AS error_message,
    DATA :events :: ARRAY AS events,
    DATA :status :: INT AS status,
    DATA :status_code :: INT AS status_code,
    _partition_by_block_id,
    {{ dbt_utils.generate_surrogate_key(
        ['VALUE :id :: STRING']
    ) }} AS testnet_transaction_results_id,
    _inserted_timestamp,
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

qualify(ROW_NUMBER() over (PARTITION BY testnet_transaction_results_id
ORDER BY
    _inserted_timestamp DESC)) = 1
