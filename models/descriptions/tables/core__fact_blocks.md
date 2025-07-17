{% docs core__fact_blocks %}
## Description
This table records all blocks produced on the Flow blockchain, capturing block-level metadata such as block height, timestamp, network, chain ID, transaction count, and parent block references. Each row represents a unique block, providing the foundational structure for all on-chain activity and supporting block-level analytics. The data is sourced directly from the Flow blockchain and is updated as new blocks are produced.

## Key Use Cases
- Analyzing block production rates and network performance
- Measuring transaction throughput and block utilization
- Investigating block-level events, forks, or anomalies
- Supporting time-series analytics and historical blockchain state reconstruction
- Serving as a join point for transaction, event, and token transfer models

## Important Relationships
- Serves as the parent table for `core.fact_transactions`, `core.fact_events`, and `core.ez_token_transfers`, which reference block height and block timestamp
- Can be joined with `core__fact_transactions` on `block_height` to analyze transactions per block
- Used by downstream models in the gold layer for time-based aggregations and network health metrics

## Commonly-used Fields
- `BLOCK_HEIGHT`: Unique identifier for each block, used for joins and ordering
- `BLOCK_TIMESTAMP`: Timestamp of block production, essential for time-series analysis
- `TX_COUNT`: Number of transactions included in the block, used for throughput and activity metrics
- `CHAIN_ID`, `NETWORK`, `NETWORK_VERSION`: Identify the network context for the block
- `PARENT_ID`: Reference to the parent block, useful for chain reorganization and lineage analysis
{% enddocs %} 