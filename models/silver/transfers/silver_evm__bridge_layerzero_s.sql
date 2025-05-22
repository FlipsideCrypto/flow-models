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

-- Chain mapping based on text names from the decoded logs
chain_names AS (
    SELECT * FROM (
        SELECT 'ethereum' as chain_name, 'ethereum' as blockchain UNION ALL
        SELECT 'bsc' as chain_name, 'bnb' as blockchain UNION ALL
        SELECT 'avalanche' as chain_name, 'avalanche' as blockchain UNION ALL
        SELECT 'polygon' as chain_name, 'polygon' as blockchain UNION ALL
        SELECT 'arbitrum' as chain_name, 'arbitrum' as blockchain UNION ALL
        SELECT 'optimism' as chain_name, 'optimism' as blockchain UNION ALL
        SELECT 'fantom' as chain_name, 'fantom' as blockchain UNION ALL
        SELECT 'flow' as chain_name, 'flow_evm' as blockchain UNION ALL
        SELECT 'celo' as chain_name, 'celo' as blockchain UNION ALL
        SELECT 'gnosis' as chain_name, 'gnosis' as blockchain UNION ALL
        SELECT 'core' as chain_name, 'core' as blockchain UNION ALL
        SELECT 'base' as chain_name, 'base' as blockchain
    )
),

-- Consolidate and classify bridge activity
layerzero_bridge_activity AS (
    SELECT
        m.tx_hash,
        m.block_timestamp,
        m.block_number,
        m.contract_address AS bridge_address,
        -- Use contractAddress from the decoded log where available
        COALESCE(
            m.decoded_log:contractAddress::string,
            m.decoded_log:destinationContractAddress::string
        ) AS contract_address,
        t.token_address,
        t.token_symbol,
        t.token_amount,
        -- Fees from fee events
        f.fee_amount AS amount_fee,
        -- Use sourceAddress or sender from the decoded log
        COALESCE(
            m.decoded_log:sourceAddress::string,
            m.decoded_log:sender::string,
            tx.origin_from_address
        ) AS source_address,
        -- Use destinationContractAddress for destination
        m.decoded_log:destinationContractAddress::string AS destination_address,
        -- Message ID for tracking across chains
        m.decoded_log:messageId::string AS message_id,
        -- Command ID for additional tracking
        m.decoded_log:commandId::string AS command_id,
        -- Direction based on event name
        CASE 
            WHEN m.event_name = 'MessageApproved' THEN 'outbound'
            WHEN m.event_name = 'MessageExecuted' THEN 'inbound'
            WHEN m.event_name = 'ContractCall' THEN 
                CASE WHEN m.decoded_log:destinationChain::string = 'flow' THEN 'inbound'
                     ELSE 'outbound' 
                END
            ELSE NULL 
        END AS direction,
        -- Source chain from decoded log (text format)
        COALESCE(
            m.decoded_log:sourceChain::string,
            CASE WHEN m.event_name = 'MessageApproved' THEN 'flow' ELSE NULL END
        ) AS source_chain_name,
        -- Destination chain from decoded log (text format)
        COALESCE(
            m.decoded_log:destinationChain::string,
            CASE WHEN m.event_name = 'MessageExecuted' THEN 'flow' ELSE NULL END
        ) AS destination_chain_name,
        -- Include payload hash for message correlation
        m.decoded_log:payloadHash::string AS payload_hash,
        -- Platform is always layerzero
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
    contract_address,
    token_address,
    token_symbol,
    token_amount AS amount,
    amount_fee,
    source_address,
    destination_address,
    direction,
    -- Map chain names to standardized blockchain names
    COALESCE(sc.blockchain, 
             CASE WHEN source_chain_name = 'flow' THEN 'flow_evm' 
                  ELSE source_chain_name 
             END) AS source_chain,
    COALESCE(dc.blockchain, 
             CASE WHEN destination_chain_name = 'flow' THEN 'flow_evm' 
                  ELSE destination_chain_name 
             END) AS destination_chain,
    platform,
    message_id,
    command_id,
    payload_hash,
    {{ dbt_utils.generate_surrogate_key(['tx_hash']) }} AS bridge_layerzero_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    layerzero_bridge_activity lba
LEFT JOIN
    chain_names sc ON lba.source_chain_name = sc.chain_name
LEFT JOIN
    chain_names dc ON lba.destination_chain_name = dc.chain_name