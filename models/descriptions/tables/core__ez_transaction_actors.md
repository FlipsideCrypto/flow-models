{% docs core__ez_transaction_actors %}
## Description
This curated table extracts all addresses involved in the events of each transaction on the Flow blockchain, generating an array of distinct actors for every transaction. Actors include authorizers, payers, proposers, and any other addresses referenced in transaction events. The table provides a normalized, analytics-ready view of transaction participants, supporting entity-level analysis and wallet attribution.

## Key Use Cases
- Identifying all participants in a transaction for compliance and risk analysis
- Supporting wallet attribution, entity mapping, and participant segmentation
- Enabling analytics on transaction complexity, multi-sig usage, and collaborative workflows
- Building dashboards for protocol, dApp, or user-level activity

## Important Relationships
- Derived from `core.fact_transactions` and `core.fact_events` by extracting and aggregating participant addresses
- Can be joined to `core.dim_labels` via `actors` for entity enrichment
- Used by curated models in DeFi, NFT, and rewards domains to provide participant context

## Commonly-used Fields
- `tx_id`: Unique identifier for the transaction
- `actors`: Array of all addresses involved in the transaction (authorizers, payers, proposers, event participants)
- `block_timestamp`: Used for time-series and participant trend analysis
{% enddocs %} 