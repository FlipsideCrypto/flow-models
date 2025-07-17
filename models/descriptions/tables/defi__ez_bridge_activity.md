{% docs defi__ez_bridge_activity %}
## Description
Enriched view of all token bridge transactions to and from the Flow blockchain, combining raw bridge activity with token symbol and USD price information. Adds business logic for easier analytics, including normalized asset values and protocol attribution. Data is sourced from bridge events and joined with curated price feeds.

## Key Use Cases
- Analyze cross-chain token flows with USD normalization
- Track protocol revenue and fee analytics in USD
- Attribute bridge activity by token symbol and protocol
- Power dashboards and user-facing analytics for cross-chain flows
- Support compliance, risk, and DeFi market research

## Important Relationships
- Sourced from `defi__fact_bridge_activity` (adds enrichment)
- Joins with price models for USD values (`amount_usd`, `amount_fee_usd`)
- Token symbol enrichment via curated price feeds
- Can be joined with `core__ez_token_transfers` for full asset movement

## Commonly-used Fields
- `tx_id`: Unique transaction identifier for bridge event
- `block_timestamp`: When the bridge transaction occurred
- `token_symbol`: Abbreviated symbol for the bridged token
- `amount`: Quantity of tokens bridged (decimal adjusted)
- `amount_usd`: Value of tokens bridged in USD
- `amount_fee_usd`: Fee charged by the bridge protocol in USD
- `platform`: Name of the bridge protocol

{% enddocs %} 