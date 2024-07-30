{{ config(
    materialized = 'view',
    tags = ['ez', 'bridge', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BRIDGE' }} }
) }}

SELECT
    tx_id,
    block_timestamp,
    block_height,
    bridge_address,
    token_address,
    gross_amount AS amount_adj,
    amount_fee,
    net_amount,
    source_address,
    destination_address,
    source_chain,
    destination_chain,
    platform,
    bridge_complete_id AS fact_bridge_transactions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__bridge_complete') }}
