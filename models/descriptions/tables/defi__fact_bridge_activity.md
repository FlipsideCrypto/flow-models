{% docs defi__fact_bridge_activity %}
## Description
Records all token bridge transactions to and from the Flow blockchain using supported bridge protocols (e.g., Blocto Teleport, Celer Bridge). Captures the full context of each cross-chain transfer, including source/destination addresses, token details, amounts, fees, and protocol metadata. Data is sourced from on-chain bridge events and normalized for analytics.

## Key Use Cases
- Analyze cross-chain token flows into and out of Flow
- Monitor bridge usage, volume, and protocol adoption
- Track user participation in bridging activity
- Attribute fees and bridge revenue by protocol
- Support compliance and risk monitoring for cross-chain transfers

## Important Relationships
- Linked to `defi__ez_bridge_activity` for enriched analytics (adds token symbols, USD values)
- Can be joined with `core__ez_token_transfers` for end-to-end asset movement
- Protocol attribution via `platform` and `bridge_address` fields
- Token-level analytics via `token_address` and `amount`

## Commonly-used Fields
- `tx_id`: Unique transaction identifier for bridge event
- `block_timestamp`: When the bridge transaction occurred
- `bridge_address`: Contract address of the bridge protocol
- `token_address`: Contract address of the bridged token
- `amount`: Quantity of tokens bridged (decimal adjusted)
- `amount_fee`: Fee charged by the bridge protocol
- `source_chain` / `destination_chain`: Blockchains involved in the transfer
- `platform`: Name of the bridge protocol

{% enddocs %} 