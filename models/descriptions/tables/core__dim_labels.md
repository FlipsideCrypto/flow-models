{% docs core__dim_labels %}
## Description
This table provides a comprehensive mapping of Flow addresses to human-readable labels, supporting entity identification and classification across the blockchain. Each row represents a unique address and its associated label, with additional metadata such as label type, subtype, and creator. Labels are sourced from both automated algorithms and manual curation, enabling robust address classification for analytics and reporting. The table is continuously updated to reflect new addresses, protocol deployments, and community contributions.

## Key Use Cases
- Enriching transaction, event, and contract data with human-readable labels for dashboards and analytics
- Segmenting addresses by type (e.g., CEX, DEX, dApp, game) and subtype (e.g., contract_deployer, hot_wallet)
- Supporting compliance, risk analysis, and wallet attribution
- Powering entity-level analytics for DeFi, NFT, and governance applications

## Important Relationships
- Can be joined to `core.fact_transactions`, `core.fact_events`, and other gold models via `address` for entity enrichment
- Used by curated models in DeFi, NFT, and rewards domains to provide address context and segmentation
- Label type and subtype fields enable advanced filtering and grouping in analytics workflows

## Commonly-used Fields
- `address`: The Flow blockchain address being labeled
- `label`: The human-readable name or tag for the address
- `label_type`: The primary category of the address (e.g., CEX, NFT, DeFi)
- `label_subtype`: The secondary classification (e.g., hot_wallet, validator)
- `creator`: The source or contributor of the label
{% enddocs %}
