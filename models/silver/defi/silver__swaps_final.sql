{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'swaps_final_id',
    incremental_strategy = 'merge',
    tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}
with
swaps_from_log AS (
    select
        block_height,
        block_timestamp,
        tx_id,
        swap_index,
        pool_address,
        pool_source,
        token_in_amount,
        token_in_key,
        token_out_amount,
        token_out_key,
        inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    from
        {{ ref('silver__swap_logs') }}
    {# INCREMENTAL FILTER #}
),
swaps_from_parsed_event AS (
    select
        tx_id,
        block_timestamp,
        block_height,
        swap_index,
        swap_contract,
        trader,
        token_out_sourcem
        token_out_contract,
        token_out_amount,
        token_in_destination,
        token_in_contract,
        token_in_amount,
        inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    from
        {{ ref('silver__swaps_s') }}
    WHERE
        tx_id NOT IN (select distinct tx_id from swaps_from_log)
    {# INCREMENTAL FILTER #}
)
{# Align col names and UNION TOGETHER
TODO - check data and make sure ok to combine #}

{# TODO - add price (and label?) data for ez view
 #}
