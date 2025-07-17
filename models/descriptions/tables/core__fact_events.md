{% docs core__fact_events %}
## Description
This table records all events emitted by transactions on the Flow blockchain. Each row represents a single event, capturing metadata such as the transaction ID, block height, timestamp, event type, emitting contract, event index, and event data payload. The table provides a granular, event-level view of on-chain activity, supporting detailed event-driven analytics and contract monitoring. Data is sourced directly from Flow transaction execution and includes both user-defined and system events.

## Key Use Cases
- Analyzing contract activity and event emissions over time
- Monitoring protocol-specific or user-defined events for dApps
- Auditing on-chain activity and reconstructing transaction flows
- Building dashboards for NFT, DeFi, and governance event tracking
- Supporting alerting and anomaly detection based on event patterns

## Important Relationships
- Linked to `core.fact_blocks` via `block_height` and `block_timestamp` for block context
- Linked to `core.fact_transactions` via `tx_id` for transaction context and execution results
- Event contract and event type fields can be joined to `core.dim_contract_labels` and other label tables for entity enrichment
- Used as a source for curated gold models in DeFi, NFT, and other domains that analyze event activity

## Commonly-used Fields
- `tx_id`: Unique identifier for the transaction emitting the event
- `event_type`: Specifies the type of event (user-defined or system)
- `event_contract`: Identifies the contract that emitted the event
- `event_data`: Contains the event payload for analytics and business logic extraction
- `block_timestamp`: Used for time-series analysis and event sequencing
- `event_index`: Orders events within a transaction for accurate reconstruction
{% enddocs %} 