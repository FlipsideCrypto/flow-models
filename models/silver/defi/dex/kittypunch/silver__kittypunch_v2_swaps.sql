{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'kittypunch_v2_swaps_id',
    tags = ['scheduled_non_core', 'kittypunch', 'dex']
) }}

-- depends on {{ ref('silver__kittypunch_v2_contracts') }}

WITH kittypunch_pairs AS (
    SELECT
        pair_address,
        token0_address,
        token1_address
    FROM
        {{ ref('silver__kittypunch_v2_contracts') }}
),

swap_events AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        contract_address,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        event_name,
        decoded_log,
        full_decoded_log,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core_evm__ez_decoded_event_logs') }}
    WHERE
        topic_0 = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'  -- Correct V2 Swap signature
        AND event_name = 'Swap'
        AND LOWER(contract_address) IN (
            SELECT LOWER(pair_address) FROM kittypunch_pairs
        )
        
    {% if is_incremental() %}
        AND modified_timestamp > (
            SELECT COALESCE(MAX(modified_timestamp), '2000-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
    {% endif %}
),

parsed_swaps AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        contract_address AS pair_contract,
        full_decoded_log AS data,
        decoded_log:sender::STRING AS sender_address,  -- From decoded_log, not origin
        decoded_log:to::STRING AS recipient_address,   -- From decoded_log
        -- V2 Uniswap style amounts
        decoded_log:amount0In::NUMBER AS amount0_in,
        decoded_log:amount0Out::NUMBER AS amount0_out,
        decoded_log:amount1In::NUMBER AS amount1_in,
        decoded_log:amount1Out::NUMBER AS amount1_out
    FROM
        swap_events
    WHERE
        decoded_log IS NOT NULL
        AND (
            decoded_log:amount0In::NUMBER > 0 
            OR decoded_log:amount0Out::NUMBER > 0 
            OR decoded_log:amount1In::NUMBER > 0 
            OR decoded_log:amount1Out::NUMBER > 0
        )
),

swap_with_tokens AS (
    SELECT
        s.block_number,
        s.block_timestamp,
        s.tx_hash,
        s.tx_position,
        s.event_index,
        s.pair_contract,
        s.data,
        s.sender_address,
        s.recipient_address,
        s.amount0_in,
        s.amount0_out,
        s.amount1_in,
        s.amount1_out,
        p.token0_address,
        p.token1_address
    FROM
        parsed_swaps s
    INNER JOIN
        kittypunch_pairs p
    ON
        LOWER(s.pair_contract) = LOWER(p.pair_address)
),

swap_details AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        pair_contract,
        data,
        sender_address,
        recipient_address,
        token0_address,
        token1_address,
        -- V2 Uniswap logic: determine input/output based on which amounts are non-zero
        CASE 
            WHEN amount0_in > 0 AND amount1_out > 0 THEN amount0_in   -- token0 → token1
            WHEN amount1_in > 0 AND amount0_out > 0 THEN amount1_in   -- token1 → token0
            ELSE 0
        END AS token_in_amount_raw,
        
        CASE 
            WHEN amount0_in > 0 AND amount1_out > 0 THEN amount1_out  -- token0 → token1
            WHEN amount1_in > 0 AND amount0_out > 0 THEN amount0_out  -- token1 → token0
            ELSE 0
        END AS token_out_amount_raw,
        
        -- Token contracts: determine based on swap direction
        CASE 
            WHEN amount0_in > 0 AND amount1_out > 0 THEN token0_address   -- Sending token0
            WHEN amount1_in > 0 AND amount0_out > 0 THEN token1_address   -- Sending token1  
            ELSE token0_address
        END AS token_in_contract,
        
        CASE 
            WHEN amount0_in > 0 AND amount1_out > 0 THEN token1_address   -- Receiving token1
            WHEN amount1_in > 0 AND amount0_out > 0 THEN token0_address   -- Receiving token0
            ELSE token1_address
        END AS token_out_contract,
        
        -- Pass through for debugging
        amount0_in,
        amount0_out, 
        amount1_in,
        amount1_out
    FROM
        swap_with_tokens
    WHERE
        (amount0_in > 0 OR amount0_out > 0 OR amount1_in > 0 OR amount1_out > 0)
),
FINAL AS (
    SELECT
        tx_hash AS tx_id,
        block_timestamp,
        block_number AS block_height,
        event_index,
        pair_contract AS swap_contract,
        sender_address AS trader,
        recipient_address,
        'kittypunch' AS platform,
        'v2' AS platform_version,
        token_in_contract,
        token_out_contract,
        token_in_amount_raw AS token_in_amount,
        token_out_amount_raw AS token_out_amount,
        CASE 
            WHEN token_in_contract = token0_address THEN 'token0_to_token1'
            WHEN token_in_contract = token1_address THEN 'token1_to_token0'
            ELSE 'unknown'
        END AS swap_direction,
        0 AS pair_id,  -- Set default pair_id since we don't extract it in this model
        CASE
            WHEN LOWER(token_in_contract) = LOWER('0xd3bF53DAC106A0290B0483EcBC89d40FcC961f3e') 
                OR LOWER(token_out_contract) = LOWER('0xd3bF53DAC106A0290B0483EcBC89d40FcC961f3e')
            THEN TRUE
            ELSE FALSE
        END AS contains_wflow,
        data as raw_data
    FROM
        swap_details
    WHERE
        token_in_amount_raw > 0 
        AND token_out_amount_raw > 0
        AND token_in_contract IS NOT NULL
        AND token_out_contract IS NOT NULL
)

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'event_index']
    ) }} AS kittypunch_v2_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL