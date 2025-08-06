{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::date'],
    unique_key = 'bridge_complete_id',
    tags = ['bridge', 'scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH
blocto AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        teleport_contract AS bridge_address,
        token_contract AS token_address,
        gross_amount,
        amount_fee,
        net_amount,
        IFF(teleport_direction = 'inbound', flow_wallet_address, null) AS destination_address,
        IFF(teleport_direction = 'outbound', flow_wallet_address, null) AS source_address,
        IFF(teleport_direction = 'inbound', 'flow', blockchain) AS destination_chain,
        IFF(teleport_direction = 'outbound', 'flow', blockchain) AS source_chain,
        bridge AS platform,
        inserted_timestamp,
        modified_timestamp,
        bridge_blocto_id AS bridge_complete_id
    FROM
        {{ ref('silver__bridge_blocto_s') }}
{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
celer AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        bridge_contract AS bridge_address,
        token_contract AS token_address,
        amount AS gross_amount,
        NULL AS amount_fee,
        amount AS net_amount,
         IFF(direction = 'inbound', flow_wallet_address, counterparty) AS destination_address,
        IFF(direction = 'outbound', flow_wallet_address, counterparty) AS source_address,
        IFF(direction = 'inbound', 'flow', blockchain) AS destination_chain,
        IFF(direction = 'outbound', 'flow', blockchain) AS source_chain,
        bridge AS platform,
        inserted_timestamp,
        modified_timestamp,
        bridge_celer_id AS bridge_complete_id
    FROM
        {{ ref('silver__bridge_celer_s') }}
{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
stargate AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        bridge_address,
        token_address,
        gross_amount,
        amount_fee,
        net_amount,
        IFF(direction = 'inbound', flow_wallet_address, null) AS destination_address,
        IFF(direction = 'outbound', flow_wallet_address, null) AS source_address,
        IFF(direction = 'inbound', 'flow', destination_chain) AS destination_chain,
        IFF(direction = 'outbound', 'flow', source_chain) AS source_chain,
        platform,
        inserted_timestamp,
        modified_timestamp,
        bridge_startgate_id AS bridge_complete_id
    FROM
        {{ ref('silver_evm__bridge_stargate_s') }}
{% if is_incremental() %}
WHERE modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
combo AS (
    SELECT
        *
    FROM
        blocto
    UNION ALL
    SELECT
        *
    FROM
        celer
        UNION ALL
    SELECT
        *
    FROM
        stargate
)
SELECT
    tx_id,
    block_timestamp,
    block_height,
    bridge_address,
    token_address,
    gross_amount,
    amount_fee,
    net_amount,
    source_address,
    destination_address,
    source_chain,
    destination_chain,
    platform,
    bridge_complete_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
FROM
    combo
