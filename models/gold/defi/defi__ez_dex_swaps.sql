{{ config (
    materialized = 'view',
    tags = ['ez', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }} }
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
    swap_contract AS contract_address,
    swap_index,
    trader,
    platform,
    token_out_source AS origin_from_address,
    token_out_contract AS token_out,
    po.symbol AS token_out_symbol,
    token_out_amount AS amount_out,
    token_out_amount * po.price AS amount_out_usd,
    token_in_destination AS origin_to_address,
    token_in_contract AS token_in,
    pi.symbol AS token_in_symbol,
    token_in_amount AS amount_in,
    token_in_amount * pi.price AS amount_in_usd,
    swaps_final_id AS ez_dex_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_final') }} s
    LEFT JOIN prices po
    ON LOWER(
        s.token_out_contract
    ) = LOWER(
        po.token_address
    )
    AND DATE_TRUNC(
        'hour',
        s.block_timestamp
    ) = po.hour
    LEFT JOIN prices pi
    ON LOWER(
        s.token_in_contract
    ) = LOWER(
        pi.token_address
    )
    AND DATE_TRUNC(
        'hour',
        s.block_timestamp
    ) = pi.hour

WHERE
    token_in_contract IS NOT NULL
