{{ config (
    materialized = 'view',
    tags = ['ez', 'scheduled'],
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'SWAPS'
            }
        }
    }
) }}

WITH chainwalkers AS (

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
    WHERE
        block_height < {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
streamline AS (
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
        {{ ref('silver__swaps_s') }}
    WHERE
        block_height >= {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
FINAL AS (
    SELECT
        *
    FROM
        chainwalkers
    UNION ALL
    SELECT
        *
    FROM
        streamline
)
SELECT
    *
FROM
    FINAL
WHERE
    token_in_destination IS NOT NULL
