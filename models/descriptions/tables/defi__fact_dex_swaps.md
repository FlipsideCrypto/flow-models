{% docs defi__fact_dex_swaps %}
## Description
Records all decentralized exchange (DEX) swap transactions on the Flow blockchain. Captures the full context of each swap, including trader, pool, token in/out, amounts, and protocol metadata. Data is sourced from on-chain swap events and normalized for analytics, supporting both single-pool and multi-hop swaps.

## Key Use Cases
- Analyze DEX trading activity and swap volume
- Track user participation and trading behavior
- Attribute swaps to specific pools and protocols
- Support price impact, slippage, and liquidity analysis
- Power DeFi dashboards and market research

## Important Relationships
- Linked to `defi__ez_dex_swaps` for enriched analytics (adds token symbols, USD values)
- Can be joined with `defi__dim_swap_pool_labels` for pool metadata
- Protocol attribution via `platform` and `contract_address` fields
- Token-level analytics via `token_in`/`token_out` and amounts

## Commonly-used Fields
- `tx_id`: Unique transaction identifier for swap event
- `block_timestamp`: When the swap occurred
- `contract_address`: DEX pool contract address
- `swap_index`: Order of swap in multi-hop transactions
- `trader`: Account address of the swap initiator
- `token_out` / `token_in`: Token contract addresses swapped out/in
- `amount_out` / `amount_in`: Amounts swapped (decimal adjusted)
- `platform`: Name of the DEX protocol

{% enddocs %} 