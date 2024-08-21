{{ config (
    materialized = 'view',
    tags = ['ez', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }} }
) }}

SELECT
    swaps_final_id AS swaps_id,
    tx_id,
    block_timestamp,
    block_height,
    swap_contract,
    swap_index,
    trader,
    token_out_source,
    token_out_contract,
    token_out_amount,
    token_in_destination,
    token_in_contract,
    token_in_amount,
    COALESCE (
        swaps_id,
        {{ dbt_utils.generate_surrogate_key(['tx_id', 'swap_index']) }}
    ) AS ez_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_final') }}
WHERE
    token_in_contract IS NOT NULL
