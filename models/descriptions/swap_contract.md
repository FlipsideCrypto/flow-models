{% docs swap_contract %}

The smart contract address of the DEX swap pool or liquidity pool involved in the transaction. Data type: STRING. This field identifies the specific contract facilitating the swap, which is essential for protocol attribution, pool-level analytics, and tracing liquidity movements. For Metapier swaps, all pools may use the same master contract (e.g., 'A.609e10301860b683.PierPair'), so the 'pool_id' is required to differentiate between pools. Example: 'A.609e10301860b683.PierPair' for Metapier, or a unique contract address for other DEXs. Important for understanding liquidity routing, protocol usage, and swap path analysis.

{% enddocs %}