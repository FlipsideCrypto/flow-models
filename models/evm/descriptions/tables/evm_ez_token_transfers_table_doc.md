{% docs evm_ez_token_transfers_table_doc %}
## Description
Easy-to-use token transfer table providing simplified access to ERC-20 and ERC-721 token transfers across EVM-compatible blockchains. This table aggregates transfer events and provides business-friendly views of token movements with USD pricing and metadata.

## Key Use Cases
- Token transfer analysis and tracking
- Portfolio movement monitoring
- Cross-chain token flow analysis
- Token holder analysis

## Important Relationships
- Links to `core_evm__fact_event_logs` for raw transfer events
- Connects to `core_evm__dim_contracts` for token metadata
- Supports token transfer analytics

## Commonly-used Fields
- `tx_hash`: Transaction hash containing the transfer
- `from_address`: Source address
- `to_address`: Destination address
- `contract_address`: Token contract address
- `amount`: Transfer amount
- `amount_usd`: Transfer value in USD
{% enddocs %} 