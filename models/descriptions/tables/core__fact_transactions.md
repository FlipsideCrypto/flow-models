{% docs core__fact_transactions %}
## Description
This table records all transactions executed on the Flow blockchain, including metadata such as transaction ID, block height, timestamp, proposer, payer, authorizers, gas limit, script, arguments, and execution results. Each row represents a unique transaction, capturing both successful and failed executions, and providing the foundation for transaction-level analytics and tracing.

## Key Use Cases
- Analyzing transaction throughput and network activity
- Investigating failed transactions and error messages
- Tracing the flow of funds and contract interactions
- Supporting DeFi, NFT, and governance analytics by linking transactions to events and token transfers
- Building dashboards for transaction monitoring and user behavior analysis

## Important Relationships
- Linked to `core.fact_blocks` via `block_height` and `block_timestamp` for block-level context
- Referenced by `core.fact_events` and `core.ez_token_transfers` for event and token transfer analysis
- Used as a base for downstream gold models in DeFi, NFT, and governance analytics

## Commonly-used Fields
- `TX_ID`: Unique identifier for each transaction, used for joins and tracing
- `BLOCK_HEIGHT`, `BLOCK_TIMESTAMP`: Provide block context for the transaction
- `PROPOSER`, `PAYER`, `AUTHORIZERS`: Identify the parties involved in the transaction
- `TX_SUCCEEDED`, `ERROR_MSG`: Indicate transaction success or failure and error details
- `GAS_LIMIT`: Specifies the maximum gas allowed for execution
{% enddocs %} 