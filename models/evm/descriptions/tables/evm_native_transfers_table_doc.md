{% docs evm_native_transfers_table_doc %}
## Description
Easy-to-use native token (FLOW) transfer table. This table provides simplified access to native blockchain token transfers across EVM-compatible blockchains with USD pricing and metadata.

## Key Use Cases
- Native token transfer analysis
- Gas fee tracking
- Cross-chain native token flows
- Network economics analysis

## Important Relationships
- Links to `core_evm__fact_transactions` for transaction details
- Connects to `core_evm__fact_blocks` for block information
- Supports native token analytics

## Commonly-used Fields
- `tx_hash`: Transaction hash
- `from_address`: Source address
- `to_address`: Destination address
- `amount`: Transfer amount in native units
- `amount_usd`: Transfer value in USD
- `gas_used`: Gas consumed by transaction
{% enddocs %} 