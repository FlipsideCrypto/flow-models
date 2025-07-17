{% docs defi__dim_swap_pool_labels %}
## Description
Models all DEX swap pool and pair creation events on the Flow blockchain. Captures metadata for each pool, including token contracts, deployment timestamp, pool ID, vault address, and swap contract. Supports pool-level analytics, liquidity tracking, and protocol attribution. Data is sourced from on-chain pool creation events and normalized for analytics.

## Key Use Cases
- Analyze DEX pool creation and liquidity provisioning
- Attribute swaps to specific pools and protocols
- Track pool deployment and vault addresses
- Support liquidity, TVL, and protocol market share analysis
- Power DeFi dashboards and pool explorer tools

## Important Relationships
- Can be joined with `defi__fact_dex_swaps` and `defi__ez_dex_swaps` for swap-level analytics
- Protocol attribution via `swap_contract` and `platform` fields
- Token-level analytics via `token0_contract` and `token1_contract`

## Commonly-used Fields
- `swap_contract`: DEX pool contract address
- `deployment_timestamp`: When the pool was deployed
- `token0_contract` / `token1_contract`: Token contracts in the pool
- `pool_id`: Unique identifier for the pool
- `vault_address`: Address holding pool assets

{% enddocs %} 