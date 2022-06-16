{{ config (
    materialized = 'view'
) }}

WITH single_swaps AS (

    SELECT
        tx_id,
        block_timestamp,
        block_height,
        swap_contract,
        trader,
        token_out_amount,
        token_out_contract,
        token_in_amount,
        token_in_contract
    FROM
        {{ ref('silver__swaps_single_trade') }}
    WHERE
        block_timestamp >= '2022-05-09'
)
SELECT
    *
FROM
    single_swaps
