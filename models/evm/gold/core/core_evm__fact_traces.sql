{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    VALUE,
    value_precise_raw,
    value_precise,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    identifier,
    DATA,
    tx_status,
    sub_traces,
    trace_status,
    error_reason,
    trace_index,
    traces_id AS fact_traces_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_evm__traces') }}
