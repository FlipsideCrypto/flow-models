{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::date'],
    unique_key = 'tx_hash',
    tags = ['bridge', 'scheduled']
) }}

WITH layerzero_message_events AS (
    -- Capture message events that indicate cross-chain activity
    SELECT
        tx_hash,
        block_timestamp,
        block_number,
        contract_address,
        event_name,
        decoded_log,
        modified_timestamp
    FROM
        {{ ref('core_evm__ez_decoded_event_logs') }}
    WHERE
        contract_address = '0xe432150cce91c13a887f7d836923d5597add8e31'
        AND event_name IN ('MessageApproved', 'MessageExecuted', 'ContractCall')
        AND block_timestamp >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT
                COALESCE(MAX(modified_timestamp), '1970-01-01'::timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
),

-- Get fee events in the same transactions
fee_events AS (
    SELECT
        e.tx_hash,
        e.decoded_log:fee::number AS fee_amount,
        e.event_name
    FROM
        {{ ref('core_evm__ez_decoded_event_logs') }} e
    JOIN
        layerzero_message_events m ON e.tx_hash = m.tx_hash
    WHERE
        e.contract_address = '0xe1844c5d63a9543023008d332bd3d2e6f1fe1043'
        AND e.event_name = 'ExecutorFeePaid'
),

-- Get token transfers in the same transactions
token_transfers AS (
    SELECT
        t.tx_hash,
        t.from_address AS token_from_address,
        t.to_address AS token_to_address,
        t.contract_address AS token_address,
        t.symbol AS token_symbol,
        t.amount AS token_amount,
        ROW_NUMBER() OVER (PARTITION BY t.tx_hash ORDER BY t.amount DESC) AS rn
    FROM
        {{ ref('core_evm__ez_token_transfers') }} t
    JOIN
        layerzero_message_events m ON t.tx_hash = m.tx_hash
),

-- Join with transaction data to get origin addresses
transactions AS (
    SELECT
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address
    FROM
        {{ ref('core_evm__fact_transactions') }}
    WHERE
        tx_hash IN (SELECT tx_hash FROM layerzero_message_events)
),

-- Chain ID mapping based on LayerZero standards
chain_ids AS (
    SELECT * FROM (
        SELECT 101 as chain_id, 'ethereum' as blockchain UNION ALL
        SELECT 102 as chain_id, 'bnb' as blockchain UNION ALL
        SELECT 106 as chain_id, 'avalanche' as blockchain UNION ALL
        SELECT 108 as chain_id, 'polygon' as blockchain UNION ALL
        SELECT 109 as chain_id, 'arbitrum' as blockchain UNION ALL
        SELECT 110 as chain_id, 'optimism' as blockchain UNION ALL
        SELECT 111 as chain_id, 'fantom' as blockchain UNION ALL
        SELECT 112 as chain_id, 'flow_evm' as blockchain UNION ALL
        SELECT 115 as chain_id, 'celo' as blockchain UNION ALL
        SELECT 125 as chain_id, 'gnosis' as blockchain UNION ALL
        SELECT 126 as chain_id, 'core' as blockchain UNION ALL
        SELECT 165 as chain_id, 'base' as blockchain
    )
),

-- Consolidate and classify bridge activity
layerzero_bridge_activity AS (
    SELECT
        m.tx_hash,
        m.block_timestamp,
        m.block_number,
        m.contract_address AS bridge_address,
        t.token_address,
        t.token_symbol,
        t.token_amount AS amount,
        f.fee_amount AS amount_fee,
        tx.origin_from_address AS source_address,
        t.token_to_address AS destination_address,
        -- Determine direction based on event name
        CASE 
            WHEN m.event_name = 'MessageApproved' THEN 'outbound'
            WHEN m.event_name = 'MessageExecuted' THEN 'inbound'
            ELSE NULL 
        END AS direction,
        -- Extract source and destination chain IDs from the message event
        -- Note: These fields need to be adjusted based on actual message structure
        CASE 
            WHEN m.event_name = 'MessageApproved' THEN 'flow_evm'
            WHEN m.event_name = 'MessageExecuted' THEN m.decoded_log:srcChainId::number
            ELSE NULL 
        END AS source_chain_id,
        CASE 
            WHEN m.event_name = 'MessageApproved' THEN m.decoded_log:dstChainId::number
            WHEN m.event_name = 'MessageExecuted' THEN 'flow_evm'
            ELSE NULL 
        END AS destination_chain_id,
        'layerzero' AS platform,
        m.modified_timestamp
    FROM
        layerzero_message_events m
    JOIN
        transactions tx ON m.tx_hash = tx.tx_hash
    LEFT JOIN
        fee_events f ON m.tx_hash = f.tx_hash
    LEFT JOIN
        token_transfers t ON m.tx_hash = t.tx_hash AND t.rn = 1
)

SELECT
    tx_hash,
    block_timestamp,
    block_number,
    bridge_address,
    token_address,
    token_symbol,
    amount,
    amount_fee,
    source_address,
    destination_address,
    direction,
    -- Map chain IDs to blockchain names
    CASE 
        WHEN source_chain_id = 'flow_evm' THEN 'flow_evm'
        ELSE sc.blockchain 
    END AS source_chain,
    CASE 
        WHEN destination_chain_id = 'flow_evm' THEN 'flow_evm'
        ELSE dc.blockchain 
    END AS destination_chain,
    platform,
    {{ dbt_utils.generate_surrogate_key(['tx_hash']) }} AS bridge_layerzero_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    layerzero_bridge_activity lba
LEFT JOIN
    chain_ids sc ON lba.source_chain_id = sc.chain_id
LEFT JOIN
    chain_ids dc ON lba.destination_chain_id = dc.chain_id