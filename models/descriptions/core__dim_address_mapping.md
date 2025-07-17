{% docs core__dim_address_mapping %}
## Description
This table maps EVM addresses to Flow addresses based on COA (Custody of Account) Creation events. Each row represents an association between a Flow address and an EVM address, enabling cross-chain identity mapping and analytics. A single Flow address may have multiple EVM addresses linked to it, reflecting multi-chain participation or asset bridging. The table is updated as new COA Creation events are detected on-chain.

## Key Use Cases
- Linking user or contract activity across Flow and EVM-compatible chains
- Supporting cross-chain analytics, wallet attribution, and identity resolution
- Enabling DeFi, NFT, and bridge analytics that require address mapping
- Auditing and monitoring asset flows between Flow and EVM ecosystems

## Important Relationships
- Can be joined to Flow transaction and event tables (e.g., `core.fact_transactions`, `core.fact_events`) via `FLOW_ADDRESS` for on-chain activity
- Can be joined to EVM-based analytics tables via `EVM_ADDRESS` for cross-chain analysis
- Used by curated models in DeFi and NFT domains to enrich user and contract analytics with cross-chain context

## Commonly-used Fields
- `FLOW_ADDRESS`: The Flow blockchain address for the user or contract
- `EVM_ADDRESS`: The associated EVM-compatible address
- `BLOCK_TIMESTAMP_ASSOCIATED`: Timestamp when the mapping was established
- `BLOCK_HEIGHT_ASSOCIATED`: Block height at which the mapping was recorded
{% enddocs %} 