-- depends_on: {{ ref('bronze_evm__testnet_blocks') }}

{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(block_number)",
    tags = ['evm_testnet']
) }}

WITH flat_txs as (
    SELECT 
        block_number, 
        partition_key,
        _inserted_timestamp,
        data
    FROM 
    {% if is_incremental() %}
    {{ ref('bronze_evm__testnet_blocks') }}
    WHERE _inserted_timestamp >= (
        SELECT 
            COALESCE(MAX(_inserted_timestamp), '1900-01-01'::TIMESTAMP) AS _inserted_timestamp
        FROM {{ this }}
    ) AND data:result:transactions[0] is not null
    {% else %}
    {{ ref('bronze_evm__FR_testnet_blocks') }}
    WHERE data:result:transactions[0] is not null
    {% endif %}    
),
bronze_transactions AS (
    SELECT 
        block_number,
        partition_key,
        index :: INT AS tx_position,
        value AS transaction_json,
        _inserted_timestamp
    FROM flat_txs,
    LATERAL FLATTEN(input => data:result:transactions) AS tx
)

SELECT 
    block_number,
    partition_key,
    tx_position,
    transaction_json,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_number','tx_position']) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM bronze_transactions
QUALIFY ROW_NUMBER() OVER (PARTITION BY transactions_id ORDER BY _inserted_timestamp DESC) = 1