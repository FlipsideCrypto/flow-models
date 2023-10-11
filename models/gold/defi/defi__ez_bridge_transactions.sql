{{ config(
    materialized = 'view',
    tags = ['ez', 'bridge', 'scheduled'],
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'BRIDGE'
            }
        }
    }
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
        tx_id,
        block_timestamp,
        block_height,
        teleport_contract AS bridge_contract,
        token_contract,
        gross_amount AS amount,
        flow_wallet_address,
        blockchain,
        teleport_direction AS direction,
        bridge
    FROM
        blocto_cw
    UNION
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
        bridge
    FROM
        celer_cw
    UNION
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        teleport_contract AS bridge_contract,
        token_contract,
        gross_amount AS amount,
        flow_wallet_address,
        blockchain,
        teleport_direction AS direction,
        bridge
    FROM
        blocto_s
    UNION
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
        bridge
    FROM
        celer_s
)
SELECT
    *
FROM
    combo
