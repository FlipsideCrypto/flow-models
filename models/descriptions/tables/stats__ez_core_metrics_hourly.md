{% docs stats__ez_core_metrics_hourly %}
## Description
Easy-to-use hourly core metrics table providing aggregated blockchain statistics. This table presents hourly snapshots of key blockchain metrics including transaction counts, block counts, fees, and unique user counts for network analysis and monitoring.

## Key Use Cases
- Network performance monitoring
- Transaction volume analysis
- User activity tracking
- Fee revenue analysis

## Important Relationships
- Links to `core__fact_blocks` for block-level data
- Connects to `core__fact_transactions` for transaction details
- Supports network analytics

## Commonly-used Fields
- `block_timestamp_hour`: Hourly timestamp for time-series analysis
- `transaction_count`: Number of transactions in the hour
- `transaction_count_success`: Number of successful transactions
- `unique_from_count`: Number of unique senders
- `total_fees_usd`: Total fees in USD
{% enddocs %} 