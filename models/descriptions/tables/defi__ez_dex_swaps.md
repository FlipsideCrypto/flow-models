{% docs defi__ez_dex_swaps %}
## Description
Enriched view of all DEX swap transactions on the Flow blockchain, combining raw swap data with token symbol and USD price information. Adds business logic for easier analytics, including normalized asset values, protocol attribution, and swap path context. Data is sourced from swap events and joined with curated price feeds.

## Key Use Cases
- Analyze DEX trading activity with USD normalization
- Track protocol revenue and fee analytics in USD
- Attribute swaps by token symbol, pool, and protocol
- Power dashboards and user-facing analytics for DeFi trading
- Support liquidity, slippage, and market research

## Important Relationships
- Sourced from `defi__fact_dex_swaps` (adds enrichment)
- Joins with price models for USD values (`amount_out_usd`, `amount_in_usd`)
- Token symbol enrichment via curated price feeds
- Can be joined with `defi__dim_swap_pool_labels` for pool metadata

## Commonly-used Fields
- `tx_id`: Unique transaction identifier for swap event
- `block_timestamp`: When the swap occurred
- `token_out_symbol` / `token_in_symbol`: Abbreviated symbols for swapped tokens
- `amount_out_usd` / `amount_in_usd`: USD value of tokens swapped
- `platform`: Name of the DEX protocol

{% enddocs %} 