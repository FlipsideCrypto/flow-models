{% docs swap_index %}

The position of the swap within a transaction, indicating the order of execution in multi-hop or multi-pool swaps. Data type: NUMBER (integer). The first swap in a transaction is indexed at 0. If a transaction routes through multiple pools, each swap is assigned an incrementing index (e.g., 0, 1, 2). Used to reconstruct swap paths, analyze routing strategies, and understand complex DeFi trades. Example: A single-pool swap has swap_index 0; a two-pool route has swaps with indices 0 and 1. Important for path analysis, DEX routing analytics, and multi-hop trade reconstruction.

{% enddocs %}