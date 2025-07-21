{% docs evm_logs_table_doc %}

## Description

A comprehensive fact table containing event log data emitted by smart contracts on EVM-compatible blockchains. This table serves as the foundation for smart contract analysis, providing detailed information about events including contract addresses, topics, data, and execution context. The table supports analysis of DeFi protocols, NFT marketplaces, and other smart contract applications across multiple EVM chains. Each record represents a single event log with complete emission details and metadata.

## Key Use Cases

- **Smart Contract Analysis**: Understanding contract behavior and event emission patterns
- **DeFi Protocol Analysis**: Tracking DeFi events, swaps, liquidity changes, and yield farming activities
- **NFT Analytics**: Monitoring NFT transfers, sales, and marketplace activities
- **Event Tracking**: Following specific event types across different contracts and chains
- **Contract Interaction Analysis**: Understanding how users interact with smart contracts
- **Cross-Chain Event Analysis**: Comparing event patterns across different EVM-compatible blockchains

## Important Relationships

- **core_evm__fact_transactions**: Links to transactions that emitted these events
- **core_evm__fact_blocks**: Links to blocks containing event logs
- **core_evm__dim_contracts**: Links to contract metadata for event source identification
- **core_evm__dim_labels**: Links to address labels for contract and user identification
- **core_evm__ez_token_transfers**: May link to curated token transfer events
- **core__fact_events**: May provide comparison data with native Flow events

## Commonly-used Fields

- **CONTRACT_ADDRESS**: Essential for contract identification and event source analysis
- **TOPIC_0**: Critical for event type identification and event categorization
- **TOPICS**: Important for event parameter analysis and filtering
- **DATA**: Key for event data extraction and parameter analysis
- **TX_HASH**: Essential for linking events to their originating transactions
- **BLOCK_TIMESTAMP**: Critical for time-series analysis and temporal event tracking

{% enddocs %}


{% docs evm_event_index %}

Event number within a transaction.

{% enddocs %}


{% docs evm_event_inputs %}

The decoded event inputs for a given event.

{% enddocs %}

{% docs evm_event_removed %}

Whether the event has been removed from the transaction.

{% enddocs %}


{% docs evm_log_id_events %}

This is the primary key for this table. This is a concatenation of the transaction hash and the event index at which the event occurred. This field can be used within other event based tables such as ```fact_transfers``` & ```ez_token_transfers```.

{% enddocs %}


{% docs evm_logs_contract_address %}

The address interacted with for a given event.

{% enddocs %}


{% docs evm_logs_contract_name %}

The name of the contract or token, where possible.

{% enddocs %}


{% docs evm_logs_data %}

The un-decoded event data.

{% enddocs %}


{% docs evm_logs_tx_hash %}

Transaction hash is a unique 66-character identifier that is generated when a transaction is executed. This field will not be unique in this table, as a given transaction can include multiple events.

{% enddocs %}


{% docs evm_topics %}

The un-decoded event input topics.

{% enddocs %}


{% docs evm_topic_0 %}

The first topic of the event, which is a unique identifier for the event.

{% enddocs %}


{% docs evm_topic_1 %}  

The second topic of the event, if applicable.

{% enddocs %}


{% docs evm_topic_2 %}

The third topic of the event, if applicable.

{% enddocs %}


{% docs evm_topic_3 %}

The fourth topic of the event, if applicable.  

{% enddocs %}