{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'increment_swaps_id',
    tags = ['scheduled_non_core', 'increment', 'dex']
) }}

WITH increment_pairs AS (
    -- Get all deployed Increment pairs
    SELECT
        event_contract AS pair_contract
    FROM
        {{ ref('silver__increment_deployed_pairs') }}
),

events AS (
    SELECT
        block_height,
        block_timestamp,
        tx_id,
        event_index,
        event_contract,
        event_type,
        event_data,
        _inserted_timestamp,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_events') }}
    WHERE
        event_contract IN (SELECT pair_contract FROM increment_pairs)
        AND event_type = 'Swap'

    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
   
),

swappair_events AS (
    SELECT
        block_height,
        block_timestamp,
        tx_id,
        event_index,
        event_contract AS swap_contract,
        NULL AS platform,
        CASE 
            WHEN event_data:amount0In::FLOAT > 0 THEN event_data:amount0In::FLOAT
            ELSE event_data:amount1In::FLOAT
        END AS token_in_amount,
        
        CASE 
            WHEN event_data:amount0In::FLOAT > 0 THEN event_data:amount0Type::STRING
            ELSE event_data:amount1Type::STRING
        END AS token_in_contract,
        
        CASE 
            WHEN event_data:amount0Out::FLOAT > 0 THEN event_data:amount0Out::FLOAT
            ELSE event_data:amount1Out::FLOAT
        END AS token_out_amount,
        
        CASE 
            WHEN event_data:amount0Out::FLOAT > 0 THEN event_data:amount0Type::STRING
            ELSE event_data:amount1Type::STRING
        END AS token_out_contract,
        
        _modified_timestamp
    FROM
        events
),

transactions AS (
    SELECT
        tx_id,
        authorizers
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        tx_id IN (SELECT DISTINCT tx_id FROM swappair_events)

    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
)

SELECT
    s.block_height,
    s.block_timestamp,
    s.tx_id,
    ROW_NUMBER() OVER (PARTITION BY s.tx_id ORDER BY s.event_index) - 1 AS swap_index,
    s.swap_contract,
    s.platform,
    t.authorizers[0]::STRING AS trader,
    s.token_in_amount,
    s.token_in_contract,
    s.token_out_amount,
    s.token_out_contract,
    {{ dbt_utils.generate_surrogate_key(['s.tx_id', 's.event_index']) }} AS increment_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    swappair_events s
LEFT JOIN
    transactions t ON s.tx_id = t.tx_id
    