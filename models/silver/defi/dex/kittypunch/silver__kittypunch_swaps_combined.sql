{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'kittypunch_swaps_combined_id',
    tags = ['scheduled_non_core', 'kittypunch', 'dex', 'combined']
) }}

-- Union all working KittyPunch swap models into a single combined model  
WITH combined_swaps AS (

SELECT
    tx_hash AS tx_id,
    block_timestamp,
    block_height,
    event_index,
    swap_contract,
    sender_address,
    recipient_address,
    platform,
    token_in_contract,
    token_out_contract,
    token_in_amount,
    token_out_amount,
    raw_data,
    kittypunch_v3_swaps_id AS source_id,
    'kittypunch_v3_swaps' AS source_table,
    inserted_timestamp AS source_inserted_timestamp,
    modified_timestamp AS source_modified_timestamp
FROM {{ ref('silver__kittypunch_v3_swaps') }}

{% if is_incremental() %}
WHERE modified_timestamp > (
    SELECT COALESCE(MAX(source_modified_timestamp), '2000-01-01'::TIMESTAMP)
    FROM {{ this }}
    WHERE source_table = 'kittypunch_v3_swaps'
)
{% endif %}

UNION ALL

SELECT
    tx_id,
    block_timestamp,
    block_height,
    event_index,
    pool_address AS swap_contract,
    user_address AS sender_address,
    NULL AS recipient_address,
    platform,
    token0_address AS token_in_contract,
    token1_address AS token_out_contract,
    token0_amount AS token_in_amount,
    token1_amount AS token_out_amount,
    raw_data,
    stablekitty_swaps_id AS source_id,
    'stablekitty_swaps' AS source_table,
    inserted_timestamp AS source_inserted_timestamp,
    modified_timestamp AS source_modified_timestamp
FROM {{ ref('silver__stablekitty_swaps') }}

{% if is_incremental() %}
WHERE modified_timestamp > (
    SELECT COALESCE(MAX(source_modified_timestamp), '2000-01-01'::TIMESTAMP)
    FROM {{ this }}
    WHERE source_table = 'stablekitty_swaps'
)
{% endif %}
)

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'event_index', 'source_table']
    ) }} AS kittypunch_swaps_combined_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    combined_swaps
ORDER BY
    block_timestamp DESC