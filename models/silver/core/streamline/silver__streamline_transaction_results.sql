-- depends_on: {{ ref('bronze__streamline_transaction_results') }}
{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    cluster_by = "_inserted_timestamp::date",
    tags = ['core']
) }}

SELECT
    block_number,
    id :: STRING AS tx_id,
    DATA : error_message AS error_message,
    DATA : events AS events,
    _partition_by_block_id,
    _inserted_timestamp
FROM
{% if is_incremental() %}
{{ ref('bronze__streamline_transaction_results') }} as t 
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_fr_transaction_results') }} as t
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY t.block_number
ORDER BY
    t._inserted_timestamp DESC)) = 1