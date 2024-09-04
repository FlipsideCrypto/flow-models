{{ config(
    materialized = 'incremental',
    unique_key = "fact_event_logs_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['inserted_timestamp :: DATE', 'ROUND(block_number, -3)'],
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['evm']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    topics,
    DATA,
    event_removed,
    tx_status,
    _log_id,
    evm_logs_id AS fact_event_logs_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_evm__logs') }}
