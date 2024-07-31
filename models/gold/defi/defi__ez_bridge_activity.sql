{{ config(
    materialized = 'view',
    tags = ['ez', 'bridge', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BRIDGE' }} }
) }}

WITH prices AS (

    SELECT
        hour,
        token_address,
        symbol,
        price
    FROM
        {{ ref('silver__complete_token_prices') }}
    UNION ALL
    SELECT
        hour,
        'A.1654653399040a61.FlowToken' AS token_address,
        symbol,
        price
    FROM
        {{ ref('silver__complete_native_prices') }}
)
SELECT
    tx_id,
    block_timestamp,
    block_height,
    bridge_address,
    b.token_address,
    p.symbol AS token_symbol,
    gross_amount AS amount,
    amount_fee,
    gross_amount * p.price AS amount_usd,
    amount_fee * p.price AS amount_fee_usd,
    source_address,
    destination_address,
    source_chain,
    destination_chain,
    platform,
    bridge_complete_id AS ez_bridge_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__bridge_complete') }} b
    LEFT JOIN prices p
    ON LOWER(
        b.token_address
    ) = LOWER(
        p.token_address
    )
    AND DATE_TRUNC(
        'hour',
        b.block_timestamp
    ) = p.hour

