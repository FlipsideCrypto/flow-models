-- depends_on: {{ ref('bronze__streamline_transaction_results') }}
{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_number >= (select min(block_number) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = "tx_id",
    cluster_by = ["block_number","_inserted_timestamp::date"],
    tags = ['streamline_load', 'core', 'scheduled_core']
) }}

SELECT
    block_number,
    id AS tx_id,
    DATA: error_message :: STRING AS error_message,
    DATA: events :: ARRAY AS events,
    DATA :status :: INT AS status,
    DATA :status_code :: INT AS status_code,
    _partition_by_block_id,
    _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transaction_results') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_transaction_results') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1
