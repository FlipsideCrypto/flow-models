{% docs core__ez_token_transfers %}
## Description
This table records all token transfers on the Flow blockchain, capturing both native and fungible token movements between accounts. Each row represents a single transfer event, including metadata such as transaction ID, block height, timestamp, sender, recipient, token contract, and amount. The table provides a normalized, analytics-ready view of token flows, supporting financial analysis, wallet tracking, and protocol monitoring.

## Key Use Cases
- Analyzing token flow and wallet activity across the Flow ecosystem
- Measuring protocol, dApp, or user-level token volumes and trends
- Supporting DeFi analytics, whale tracking, and compliance monitoring
- Building dashboards for token distribution, holder analysis, and transfer patterns

## Important Relationships
- Derived from `core.fact_events` by extracting token transfer events and normalizing fields
- Can be joined to `core.dim_labels` via `sender` or `recipient` for entity enrichment
- Token contract field can be joined to contract label tables for protocol-level analytics
- Used as a source for curated gold models in DeFi, rewards, and NFT analytics

## Commonly-used Fields
- `tx_id`: Unique identifier for the transaction containing the transfer
- `sender`: The Flow address sending tokens
- `recipient`: The Flow address receiving tokens
- `token_contract`: The contract address of the transferred token
- `amount`: The quantity of tokens transferred (decimal adjusted)
- `block_timestamp`: Used for time-series and trend analysis
{% enddocs %} 