{{ config (
    materialized = 'view',
    tags = ['ez'],
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'SWAPS'
            }
        }
    }
) }}

SELECT
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
    token_in_amount
FROM
    {{ ref('silver__swaps') }}
