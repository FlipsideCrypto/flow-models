{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    VALUE,
    value_precise_unadj AS value_precise_raw,
    value_precise_adj AS value_precise,
    tx_fee,
    tx_fee_precise,
    gas_price_adj AS gas_price,
    effective_gas_price,
    gas AS gas_limit,
    gas_used,
    cumulative_gas_used,
    input_data,
    tx_status AS status,
    r,
    s,
    v,
    evm_txs_id AS fact_transactions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_evm__transactions') }}
