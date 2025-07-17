{% docs stats__ez_core_metrics_hourly %}

## Description

A comprehensive metrics table that aggregates core blockchain performance indicators on an hourly basis. This table provides key network health and performance metrics including block counts, transaction volumes, success rates, user activity, and fee collection data. The metrics are calculated using various aggregate functions (SUM, COUNT, MIN, MAX) from the core fact tables and are updated as new data arrives. This table serves as the primary source for network performance monitoring, trend analysis, and blockchain health assessment.

## Key Use Cases

- **Network Performance Monitoring**: Tracking blockchain throughput, transaction processing rates, and network efficiency
- **Health Assessment**: Monitoring transaction success rates, failure patterns, and network reliability
- **User Activity Analysis**: Understanding user engagement levels and network participation patterns
- **Economic Analysis**: Tracking fee collection, revenue generation, and network economics
- **Trend Analysis**: Identifying performance trends, seasonal patterns, and growth indicators
- **Alerting and Monitoring**: Supporting automated monitoring systems for network health and performance

## Important Relationships

- **core__fact_blocks**: Source data for block-related metrics and block range analysis
- **core__fact_transactions**: Source data for transaction counts, success rates, and user activity metrics
- **core__fact_events**: May provide additional context for transaction analysis
- **price__ez_prices_hourly**: May link to FLOW price data for fee value analysis
- **silver_stats__core_metrics_hourly**: May provide additional statistical context and validation

## Commonly-used Fields

- **BLOCK_TIMESTAMP_HOUR**: Essential for time-series analysis and temporal data aggregation
- **TRANSACTION_COUNT**: Core metric for network activity and throughput analysis
- **TRANSACTION_COUNT_SUCCESS/FAILED**: Critical for network health and reliability assessment
- **UNIQUE_FROM_COUNT/PAYER_COUNT**: Important for user activity and network participation analysis
- **TOTAL_FEES_NATIVE/USD**: Key for network economics and revenue analysis
- **BLOCK_COUNT**: Fundamental metric for blockchain throughput and performance 

{% enddocs %}