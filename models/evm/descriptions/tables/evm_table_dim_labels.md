{% docs evm_table_dim_labels %}
## Description
EVM blockchain address labels and metadata for contract and wallet identification. This table provides human-readable labels and categorization for addresses across EVM-compatible blockchains, enabling easier analysis and identification of contracts, wallets, and other blockchain entities.

## Key Use Cases
- Address identification and categorization across EVM chains
- Contract and wallet labeling for analytics
- Cross-chain address mapping and identification
- Entity relationship analysis and tracking

## Important Relationships
- Links to `core_evm__fact_transactions` for transaction analysis
- Connects to `core_evm__fact_event_logs` for event analysis
- Supports address-based joins across all EVM tables

## Commonly-used Fields
- `address`: The blockchain address being labeled
- `label`: Human-readable name or description
- `label_type`: Category of the label (contract, wallet, etc.)
- `blockchain`: The specific EVM blockchain
{% enddocs %} 