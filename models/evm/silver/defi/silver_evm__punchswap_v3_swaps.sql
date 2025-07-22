{{ config(
    materialized = 'incremental',
    unique_key = 'punchswap_v3_swap_id',
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
    WHERE TO_ADDRESS = '0xf331959366032a634c7cacf5852fe01ffdb84af0'  -- PunchSwap V3 Factory
        AND TX_SUCCEEDED = true
        AND ORIGIN_FUNCTION_SIGNATURE IS NOT NULL

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
        AND topic_0 = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'  -- Swap event (V3)
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
        utils.udf_hex_to_int(SUBSTR(e.DATA, 1, 64)) as sender,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 65, 64)) as recipient,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 129, 64)) as amount0,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 193, 64)) as amount1,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 257, 64)) as sqrt_price_x96,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 321, 64)) as liquidity,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 385, 64)) as tick
    FROM events e
    JOIN transactions t ON e.tx_hash = t.tx_hash
)
SELECT
    block_number AS block_height,
    block_timestamp,
    tx_hash AS tx_id,
    event_index AS swap_index,
    pool_address AS swap_contract,
    'PunchSwap V3' AS platform,
    trader,
    CASE 
        WHEN amount0 > 0 THEN amount0
        ELSE amount1
    END AS token_in_amount,
    NULL AS token_in_contract,
    NULL AS token_in_destination,
    CASE 
        WHEN amount0 < 0 THEN ABS(amount0)
        ELSE ABS(amount1)
    END AS token_out_amount,
    NULL AS token_out_contract,
    NULL AS token_out_source,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS punchswap_v3_swap_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM swap_events
