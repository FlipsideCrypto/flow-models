{{ config(
    materialized = 'incremental',
    unique_key = 'punchswap_v2_swap_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::date', 'modified_timestamp::date'],
    tags = ['scheduled_non_core']
) }}

WITH transactions AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        FROM_ADDRESS as trader,
        TO_ADDRESS as router_address,
        ORIGIN_FUNCTION_SIGNATURE as function_sig,
        VALUE as native_value,
        TX_SUCCEEDED as tx_succeeded
    FROM {{ ref('core_evm__fact_transactions') }}
    WHERE TO_ADDRESS = '0xf45afe28fd5519d5f8c1d4787a4d5f724c0efa4d'  -- PunchSwap V2 Router
        AND TX_SUCCEEDED = true
        AND ORIGIN_FUNCTION_SIGNATURE IN (
            '0x38ed1739', -- swapExactTokensForTokens
            '0x7ff36ab5', -- swapExactETHForTokens
            '0x18cbafe5', -- swapExactTokensForETH
            '0x8803dbee', -- swapTokensForExactTokens
            '0xfb3bdb41'  -- swapTokensForExactETH
        )

{% if is_incremental() %}
AND block_timestamp >= (
    SELECT MAX(block_timestamp)
    FROM {{ this }}
)
{% endif %}
),
events AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        contract_address,
        event_index,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        DATA
    FROM {{ ref('core_evm__fact_event_logs') }}
    WHERE tx_hash IN (SELECT tx_hash FROM transactions)
        AND topic_0 = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'  -- Swap event
),
swap_events AS (
    SELECT
        e.tx_hash,
        e.block_number,
        e.block_timestamp,
        e.contract_address as pool_address,
        e.event_index,
        '0x' || SUBSTR(t.trader, 3) as trader,
        t.function_sig,
        t.native_value,
        CASE 
            WHEN t.function_sig = '0x7ff36ab5' THEN 'ETH_TO_TOKEN'
            WHEN t.function_sig = '0x18cbafe5' THEN 'TOKEN_TO_ETH'
            ELSE 'TOKEN_TO_TOKEN'
        END as swap_type,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 1, 64)) as amount0_in,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 65, 64)) as amount1_in,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 129, 64)) as amount0_out,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 193, 64)) as amount1_out
    FROM events e
    JOIN transactions t ON e.tx_hash = t.tx_hash
)
SELECT
    block_number AS block_height,
    block_timestamp,
    tx_hash AS tx_id,
    event_index AS swap_index,
    pool_address AS swap_contract,
    'PunchSwap V2' AS platform,
    trader,
    CASE 
        WHEN swap_type = 'ETH_TO_TOKEN' THEN native_value
        ELSE amount0_in
    END AS token_in_amount,
    CASE 
        WHEN swap_type = 'ETH_TO_TOKEN' THEN '0x0000000000000000000000000000000000000000'
        WHEN amount0_in > 0 THEN '0x' || SUBSTR(topic_1, 27)
        ELSE '0x' || SUBSTR(topic_2, 27)
    END AS token_in_contract,
    NULL AS token_in_destination,
    CASE 
        WHEN swap_type = 'TOKEN_TO_ETH' THEN native_value
        ELSE amount0_out
    END AS token_out_amount,
    CASE 
        WHEN swap_type = 'TOKEN_TO_ETH' THEN '0x0000000000000000000000000000000000000000'
        WHEN amount0_out > 0 THEN '0x' || SUBSTR(topic_1, 27)
        ELSE '0x' || SUBSTR(topic_2, 27)
    END AS token_out_contract,
    NULL AS token_out_source,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS punchswap_v2_swap_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM swap_events
