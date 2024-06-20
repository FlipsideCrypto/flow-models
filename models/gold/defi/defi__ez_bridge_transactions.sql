{{ config(
    materialized = 'view',
    tags = ['ez', 'bridge', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BRIDGE' }} }
) }}

WITH
blocto_s AS (
    SELECT
        *
    FROM
        {{ ref('silver__bridge_blocto_s') }}
),
celer_s AS (
    SELECT
        *
    FROM
        {{ ref('silver__bridge_celer_s') }}
),
combo AS (
    SELECT
        bridge_blocto_id AS bridge_id,
        tx_id,
        block_timestamp,
        block_height,
        teleport_contract AS bridge_contract,
        token_contract,
        gross_amount AS amount,
        flow_wallet_address,
        blockchain,
        teleport_direction AS direction,
        bridge,
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp
    FROM
        blocto_s
    UNION ALL
    SELECT
        bridge_celer_id AS bridge_id,
        tx_id,
        block_timestamp,
        block_height,
        bridge_contract,
        token_contract,
        amount,
        flow_wallet_address,
        blockchain,
        direction,
        bridge,
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp
    FROM
        celer_s
)
SELECT
    tx_id,
    block_timestamp,
    block_height,
    bridge_contract,
    token_contract,
    amount,
    flow_wallet_address,
    blockchain,
    direction,
    bridge,
    COALESCE (
        bridge_id,
        {{ dbt_utils.generate_surrogate_key(['tx_id']) }}
    ) AS ez_bridge_transactions_id,
    COALESCE (
        inserted_timestamp,
        _inserted_timestamp
    ) AS inserted_timestamp,
    COALESCE (
        modified_timestamp,
        _inserted_timestamp
    ) AS modified_timestamp
FROM
    combo