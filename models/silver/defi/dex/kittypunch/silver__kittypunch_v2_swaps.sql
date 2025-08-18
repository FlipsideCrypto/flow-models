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
        topics,
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        data,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('core_evm__fact_event_logs') }}
    WHERE
        topic_0 = '0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1'
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
        TRY_CAST(utils.udf_hex_to_int(SUBSTR(data, 3, 64)) AS NUMBER) AS amount0,
        TRY_CAST(utils.udf_hex_to_int(SUBSTR(data, 67, 64)) AS NUMBER) AS amount1
    FROM
        swap_events
    WHERE
        data IS NOT NULL
),

swap_with_tokens AS (
    SELECT
        s.block_number,
        s.block_timestamp,
        s.tx_hash,
        s.tx_position,
        s.event_index,
        s.pair_contract,
        t.from_address AS sender_address,
        s.amount0,
        s.amount1,
        p.token0_address,
        p.token1_address,
    FROM
        parsed_swaps s
    INNER JOIN
        kittypunch_pairs p
    ON
        LOWER(s.pair_contract) = LOWER(p.pair_address)
    INNER JOIN
        {{ ref('core_evm__fact_transactions') }} t
    ON
        s.tx_hash = t.tx_hash
),

swap_details AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        tx_position,
        event_index,
        pair_contract,
        sender_address,
        token0_address,
        token1_address,
        CASE 
            WHEN amount0 > 0 THEN amount0
            ELSE amount1
        END AS token_in_amount_raw,
        
        CASE 
            WHEN amount0 > 0 THEN amount1
            ELSE amount0
        END AS token_out_amount_raw,
        
        -- Token contracts based on amounts
        CASE 
            WHEN amount0 > 0 THEN token0_address
            ELSE token1_address
        END AS token_in_contract,
        
        CASE 
            WHEN amount0 > 0 THEN token1_address
            ELSE token0_address
        END AS token_out_contract,
    FROM
        swap_with_tokens
    WHERE
        amount0 IS NOT NULL AND amount1 IS NOT NULL 
        AND (amount0 > 0 OR amount1 > 0)
),
FINAL (
    SELECT
        tx_hash AS tx_id,
        block_timestamp,
        block_number AS block_height,
        event_index,
        pair_contract AS swap_contract,
        sender_address,
        'kittypunch_v2' AS platform,
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
    ) }} AS kittypunch_v2_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
ORDER BY
    block_timestamp DESC