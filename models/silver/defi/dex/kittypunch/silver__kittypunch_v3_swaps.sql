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
        tx_position,
        event_index,
        contract_address AS pool_address,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS sender_address,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS recipient_address,
        data,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core_evm__fact_event_logs') }}
    WHERE
        topic_0 = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67' -- V3 Swap event signature
        AND LOWER(contract_address) IN (
            SELECT LOWER(pool_address) FROM kittypunch_pools
        )
        AND block_timestamp >= '2025-04-01' -- V3 deployment
        
    {% if is_incremental() %}
        AND modified_timestamp > (
            SELECT COALESCE(MAX(modified_timestamp), '2025-04-01'::TIMESTAMP)
            FROM {{ this }}
        )
    {% endif %}
),
transfer_events AS (
    SELECT 
        t.tx_hash,
        t.event_index,
        t.block_number,
        t.block_timestamp,
        CONCAT('0x', SUBSTR(t.topic_1, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(t.topic_2, 27, 40)) AS to_address,
        TRY_CAST(utils.udf_hex_to_int(SUBSTR(t.data, 3, 64)) AS NUMBER) AS amount,
        t.contract_address AS token_address,
        s.pool_address,
        s.sender_address,
        s.recipient_address,
        s.event_index AS swap_event_index,
        s.data,
        s.inserted_timestamp,
        s.modified_timestamp
    FROM {{ ref('core_evm__fact_event_logs') }} t
    INNER JOIN v3_swap_events s ON t.tx_hash = s.tx_hash
    WHERE t.topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- Swap event
        AND t.data IS NOT NULL
        AND (LOWER(CONCAT('0x', SUBSTR(t.topic_1, 27, 40))) = LOWER(s.pool_address)  -- FROM pool
             OR LOWER(CONCAT('0x', SUBSTR(t.topic_2, 27, 40))) = LOWER(s.pool_address)) -- TO pool
),
swap_amounts AS (
    SELECT 
        tx_hash,
        pool_address,
        sender_address,
        recipient_address,
        swap_event_index,
        data,
        block_number,
        block_timestamp,
        MAX(CASE WHEN LOWER(from_address) = LOWER(pool_address) THEN token_address END) AS token_out_contract,
        MAX(CASE WHEN LOWER(from_address) = LOWER(pool_address) THEN amount END) AS token_out_amount_raw,
        MAX(CASE WHEN LOWER(to_address) = LOWER(pool_address) THEN token_address END) AS token_in_contract,
        MAX(CASE WHEN LOWER(to_address) = LOWER(pool_address) THEN amount END) AS token_in_amount_raw,
    FROM transfer_events
    WHERE amount IS NOT NULL
    GROUP BY 
        tx_hash, pool_address, sender_address, recipient_address, swap_event_index, data,
        block_number, block_timestamp
    HAVING 
        token_out_contract IS NOT NULL 
        AND token_in_contract IS NOT NULL
        AND token_out_amount_raw > 0
        AND token_in_amount_raw > 0
        AND token_out_amount_raw IS NOT NULL
        AND token_in_amount_raw IS NOT NULL
),
swap_details AS (
    SELECT
        s.tx_hash,
        s.block_timestamp,
        s.block_number,
        s.swap_event_index AS event_index,
        s.pool_address AS pool_contract,
        s.sender_address,
        s.recipient_address,
        s.token_in_contract,
        s.token_out_contract, 
        s.token_in_amount_raw,
        s.token_out_amount_raw,
        s.data,
        p.token0_address,
        p.token1_address
    FROM
        swap_amounts s
    INNER JOIN
        kittypunch_pools p
    ON
        LOWER(s.pool_address) = LOWER(p.pool_address)
    WHERE
        s.token_in_contract != s.token_out_contract
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
        ['tx_hash', 'event_index']
    ) }} AS kittypunch_v3_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
ORDER BY
    block_timestamp DESC