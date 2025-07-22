{{ config(
    materialized = 'incremental',
    unique_key = 'stablekitty_swap_id',
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
    WHERE TO_ADDRESS = '0x09d35647cedc6725696e330be485ccc0d3385819'  -- StableKitty Router
        AND TX_SUCCEEDED = true
        AND ORIGIN_FUNCTION_SIGNATURE = '0xfd44959c'  -- exchange function

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
        AND topic_0 = '0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140'  -- TokenExchange event
),
exchange_events AS (
    SELECT
        e.tx_hash,
        e.block_number,
        e.block_timestamp,
        e.contract_address as pool_address,
        e.event_index,
        '0x' || SUBSTR(t.trader, 3) as trader,
        t.function_sig,
        t.native_value,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 1, 64)) as buyer,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 65, 64)) as sold_id,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 129, 64)) as tokens_sold,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 193, 64)) as bought_id,
        utils.udf_hex_to_int(SUBSTR(e.DATA, 257, 64)) as tokens_bought
    FROM events e
    JOIN transactions t ON e.tx_hash = t.tx_hash
)
SELECT
    block_number AS block_height,
    block_timestamp,
    tx_hash AS tx_id,
    event_index AS swap_index,
    pool_address AS swap_contract,
    'StableKitty' AS platform,
    trader,
    tokens_sold AS token_in_amount,
    CONCAT('0x', LPAD(HEX(sold_id), 40, '0')) AS token_in_contract,
    NULL AS token_in_destination,
    tokens_bought AS token_out_amount,
    CONCAT('0x', LPAD(HEX(bought_id), 40, '0')) AS token_out_contract,
    NULL AS token_out_source,
    {{ dbt_utils.generate_surrogate_key(['tx_hash', 'event_index']) }} AS stablekitty_swap_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM exchange_events
