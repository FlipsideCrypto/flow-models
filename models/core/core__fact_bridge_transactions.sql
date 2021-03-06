{{ config(
    materialized = 'view'
) }}

WITH blocto AS (

    SELECT
        *
    FROM
        {{ ref('silver__bridge_blocto') }}
),
celer AS (
    SELECT
        *
    FROM
        {{ ref('silver__bridge_celer') }}
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
        blocto
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
        celer
)
SELECT
    *
FROM
    combo
WHERE
    block_timestamp >= '2022-04-20'
