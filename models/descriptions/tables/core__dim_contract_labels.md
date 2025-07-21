{% docs core__dim_contract_labels %}
## Description
This table contains all contract labels referenced in the events of Flow transactions. Each row represents a unique contract, identified by its event contract address and contract name, and is enriched with account address metadata. The table provides a canonical mapping of contracts to their human-readable names and associated addresses, supporting entity resolution and contract-level analytics across the Flow blockchain.

## Key Use Cases
- Enriching event and transaction data with contract labels for analytics and dashboards
- Supporting entity resolution and contract-level segmentation in DeFi, NFT, and governance analytics
- Enabling contract-level filtering, grouping, and reporting in business intelligence tools
- Auditing and monitoring contract activity and deployments on Flow

## Important Relationships
- Can be joined to `core.fact_events` via `event_contract` to enrich event data with contract names and addresses
- Can be joined to `core.fact_transactions` via contract addresses for transaction-level analytics
- Used by curated gold models in DeFi, NFT, and other domains to provide contract context and labeling

## Commonly-used Fields
- `event_contract`: The Flow contract address emitting events
- `contract_name`: The human-readable name of the contract
- `account_address`: The account address associated with the contract
{% enddocs %} 