{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'kittypunch_v3_swaps_id',
    tags = ['scheduled_non_core', 'kittypunch', 'dex']
) }}

-- depends on {{ ref('silver__kittypunch_v3_contracts') }}

WITH kittypunch_pools AS (
    SELECT
        pool_address,
        token0_address,
        token1_address
    FROM
        {{ ref('silver__kittypunch_v3_contracts') }}
),
v3_swap_events AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address AS pool_address,
        -- Extract sender and recipient from indexed topics
        LOWER(CONCAT('0x', SUBSTR(topics[1], 27))) AS sender_address,
        LOWER(CONCAT('0x', SUBSTR(topics[2], 27))) AS recipient_address,
        -- Decode amounts from hex data with error handling
        TRY_CAST(utils.udf_hex_to_int(SUBSTR(data, 3, 64)) AS NUMBER) AS amount0,
        TRY_CAST(utils.udf_hex_to_int(SUBSTR(data, 67, 64)) AS NUMBER) AS amount1,
        data AS raw_data,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core_evm__fact_event_logs') }}
    WHERE
        topic_0 = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
        AND LOWER(contract_address) IN (
            SELECT LOWER(pool_address) FROM kittypunch_pools
        )
        AND block_timestamp >= '2024-01-01'
        AND tx_succeeded = TRUE
        
    {% if is_incremental() %}
        AND modified_timestamp > (
            SELECT COALESCE(MAX(modified_timestamp), '2025-04-01'::TIMESTAMP)
            FROM {{ this }}
        )
    {% endif %}
),
swap_amounts AS (
    SELECT 
        tx_hash,
        pool_address,
        sender_address,
        recipient_address,
        event_index,
        raw_data,
        block_number,
        block_timestamp,
        -- V3 logic: positive amount = outgoing, negative = incoming
        CASE 
            WHEN amount0 > 0 THEN amount0
            WHEN amount1 > 0 THEN amount1
            ELSE 0
        END AS token_in_amount_raw,
        CASE 
            WHEN amount0 < 0 THEN ABS(amount0)
            WHEN amount1 < 0 THEN ABS(amount1)
            ELSE 0
        END AS token_out_amount_raw,
        -- Determine token contracts based on amount direction
        CASE 
            WHEN amount0 > 0 OR amount0 < 0 THEN 'token0'
            ELSE 'token1'
        END AS input_token,
        CASE 
            WHEN amount0 < 0 OR amount0 > 0 THEN 'token1'
            ELSE 'token0'
        END AS output_token,
        amount0,
        amount1,
        inserted_timestamp,
        modified_timestamp
    FROM v3_swap_events
    WHERE amount0 IS NOT NULL AND amount1 IS NOT NULL 
        AND (amount0 != 0 OR amount1 != 0)
),
swap_details AS (
    SELECT
        s.tx_hash,
        s.block_timestamp,
        s.block_number,
        s.event_index,
        s.pool_address AS pool_contract,
        s.sender_address,
        s.recipient_address,
        -- Map token contracts based on direction
        CASE 
            WHEN s.input_token = 'token0' THEN p.token0_address
            ELSE p.token1_address
        END AS token_in_contract,
        CASE 
            WHEN s.output_token = 'token0' THEN p.token0_address  
            ELSE p.token1_address
        END AS token_out_contract,
        s.token_in_amount_raw,
        s.token_out_amount_raw,
        s.raw_data
    FROM
        swap_amounts s
    INNER JOIN
        kittypunch_pools p ON LOWER(s.pool_address) = LOWER(p.pool_address)
    WHERE
        p.token0_address != p.token1_address
        AND s.token_in_amount_raw > 0 
        AND s.token_out_amount_raw > 0
),
FINAL AS (
    SELECT
        tx_hash,
        block_timestamp,
        block_number AS block_height,
        event_index,
        pool_contract AS swap_contract,
        sender_address,
        recipient_address,
        'kittypunch_v3' AS platform,
        token_in_contract,
        token_out_contract,
        token_in_amount_raw AS token_in_amount,
        token_out_amount_raw AS token_out_amount,
        raw_data
    FROM swap_details
)

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS kittypunch_v3_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
ORDER BY
    block_timestamp DESC