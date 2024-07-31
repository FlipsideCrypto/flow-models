{{ config (
    materialized = 'view',
    tags = ['ez', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }} }
) }}

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
    token_out_amount AS amount_out,
    token_in_destination AS origin_to_address,
    token_in_contract AS token_in,
    token_in_amount AS amount_in,
    swaps_final_id AS fact_dex_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_final') }}
WHERE
    token_in_contract IS NOT NULL
