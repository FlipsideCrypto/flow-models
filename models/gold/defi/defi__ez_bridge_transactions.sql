{{ config(
    materialized = 'view',
    tags = ['ez', 'bridge', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BRIDGE' }} }
) }}

WITH blocto_cw AS (

    SELECT
        *
    FROM
        {{ ref('silver__bridge_blocto') }}
    WHERE
        block_height < {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
celer_cw AS (
    SELECT
        *
    FROM
        {{ ref('silver__bridge_celer') }}
    WHERE
        block_height < {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
blocto_s AS (
    SELECT
        *
    FROM
        {{ ref('silver__bridge_blocto_s') }}
    WHERE
        block_height >= {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
celer_s AS (
    SELECT
        *
    FROM
        {{ ref('silver__bridge_celer_s') }}
    WHERE
        block_height >= {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
combo AS (
    SELECT
        NULL AS bridge_id,
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
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        blocto_cw
    UNION ALL
    SELECT
        NULL AS bridge_id,
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
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        celer_cw
    UNION ALL
    SELECT
        bridge_blockto_id AS bridge_id,
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
    COALESCE (
        bridge_id,
        {{ dbt_utils.generate_surrogate_key(['tx_id']) }}
    ) AS bridge_id,
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
        inserted_timestamp,
        _inserted_timestamp
    ) AS inserted_timestamp,
    COALESCE (
        modified_timestamp,
        _inserted_timestamp
    ) AS modified_timestamp
FROM
    combo qualify ROW_NUMBER() over (
        PARTITION BY bridge_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
