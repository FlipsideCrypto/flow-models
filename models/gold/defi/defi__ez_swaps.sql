{{ config (
    materialized = 'view',
    tags = ['ez', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }}}
) }}

WITH chainwalkers AS (

    SELECT
        NULL AS swaps_id,
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
        _inserted_timestamp,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        {{ ref('silver__swaps') }}
    WHERE
        block_height < {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
streamline AS (
    SELECT
        swaps_id,
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
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp
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
    COALESCE (
        inserted_timestamp,
        _inserted_timestamp
    ) AS inserted_timestamp,
    COALESCE (
        modified_timestamp,
        _inserted_timestamp
    ) AS modified_timestamp
FROM
    FINAL
WHERE
    token_in_destination IS NOT NULL qualify ROW_NUMBER() over (
        PARTITION BY swaps_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
