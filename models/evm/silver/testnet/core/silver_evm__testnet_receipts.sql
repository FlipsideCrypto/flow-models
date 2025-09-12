-- depends_on: {{ ref('bronze_evm__testnet_receipts') }}
-- depends_on: {{ ref('bronze_evm__FR_testnet_receipts') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    tags = ['evm_testnet']
) }}

WITH bronze AS (

    SELECT
        block_number,
        DATA,
        partition_key,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze_evm__testnet_receipts') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    ) AND data:result[0] is not null
{% else %}
    {{ ref('bronze_evm__FR_testnet_receipts') }}
    WHERE data:result[0] is not null
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
),
flat_receipts AS (
    SELECT 
        block_number,
        partition_key,
        index :: INT AS array_index,
        value AS receipts_json,
        _inserted_timestamp
    FROM bronze,
    LATERAL FLATTEN(input => data:result) AS receipt
)
SELECT 
    block_number,
    partition_key,
    array_index,
    receipts_json,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_number','array_index']) }} AS receipts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM flat_receipts

QUALIFY(ROW_NUMBER() OVER (PARTITION BY block_number, array_index ORDER BY _inserted_timestamp DESC)) = 1