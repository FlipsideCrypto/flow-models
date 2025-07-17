{% docs stats__ez_core_metrics_hourly %}

{{ doc("tables/stats__ez_core_metrics_hourly") }}

{% enddocs %}

{% docs block_timestamp_hour %}

The hour timestamp representing the aggregation period for core blockchain metrics. Data type: TIMESTAMP. This field provides the temporal reference for hourly aggregated metrics and is used for time-series analysis and chronological data organization. Used for hourly performance tracking, trend analysis, and temporal data aggregation. Example: '2024-01-15 14:00:00' for the hour starting at 2 PM on January 15, 2024. Critical for time-series analytics, performance monitoring, and maintaining temporal consistency in blockchain metrics analysis.

{% enddocs %}

{% docs block_number_min %}

The minimum block number within the specified hour period. Data type: INTEGER. This field represents the lowest block number processed during the hour and is used for block range analysis and data completeness verification. Used for block range tracking, data gap identification, and understanding blockchain progression within time periods. Example: 12345678 for the first block in the hour. Critical for data quality assessment, block progression monitoring, and maintaining accurate blockchain state tracking.

{% enddocs %}

{% docs block_number_max %}

The maximum block number within the specified hour period. Data type: INTEGER. This field represents the highest block number processed during the hour and is used for block range analysis and data completeness verification. Used for block range tracking, data gap identification, and understanding blockchain progression within time periods. Example: 12345999 for the last block in the hour. Critical for data quality assessment, block progression monitoring, and maintaining accurate blockchain state tracking.

{% enddocs %}

{% docs block_count %}

The total number of blocks processed within the specified hour period. Data type: INTEGER. This field represents the count of blocks that were created and processed during the hour, providing a key metric for blockchain throughput and network activity. Used for network performance analysis, throughput monitoring, and understanding blockchain activity levels. Example: 321 for 321 blocks processed in the hour. Critical for network health monitoring, performance benchmarking, and understanding blockchain scalability and efficiency.

{% enddocs %}

{% docs transaction_count %}

The total number of transactions processed within the specified hour period. Data type: INTEGER. This field represents the count of all transactions that were submitted and processed during the hour, regardless of success or failure status. Used for transaction volume analysis, network activity monitoring, and understanding user engagement levels. Example: 15420 for 15,420 transactions processed in the hour. Critical for network performance analysis, user activity tracking, and understanding blockchain adoption and usage patterns.

{% enddocs %}

{% docs transaction_count_success %}

The number of transactions that were successfully processed within the specified hour period. Data type: INTEGER. This field represents the count of transactions that completed successfully without errors or failures. Used for success rate analysis, network reliability monitoring, and understanding transaction processing efficiency. Example: 15200 for 15,200 successful transactions out of 15,420 total transactions. Critical for network health assessment, user experience analysis, and identifying potential network issues or bottlenecks.

{% enddocs %}

{% docs transaction_count_failed %}

The number of transactions that failed to process within the specified hour period. Data type: INTEGER. This field represents the count of transactions that encountered errors or failures during processing. Used for failure rate analysis, network issue identification, and understanding transaction processing reliability. Example: 220 for 220 failed transactions out of 15,420 total transactions. Critical for network health monitoring, error pattern analysis, and identifying potential network issues or user experience problems.

{% enddocs %}

{% docs unique_from_count %}

The number of unique proposer addresses that submitted transactions within the specified hour period. Data type: INTEGER. This field represents the count of distinct accounts that acted as transaction proposers during the hour. Used for user activity analysis, network participation monitoring, and understanding user engagement patterns. Example: 8500 for 8,500 unique proposer addresses in the hour. Critical for understanding network decentralization, user adoption patterns, and identifying active user communities.

{% enddocs %}

{% docs unique_payer_count %}

The number of unique payer addresses that funded transactions within the specified hour period. Data type: INTEGER. This field represents the count of distinct accounts that acted as transaction payers during the hour. Used for user activity analysis, network participation monitoring, and understanding transaction funding patterns. Example: 8200 for 8,200 unique payer addresses in the hour. Critical for understanding network decentralization, user adoption patterns, and identifying accounts that fund transaction operations.

{% enddocs %}

{% docs total_fees_native %}

The total sum of all transaction fees collected within the specified hour period, denominated in the native blockchain currency (FLOW). Data type: NUMBER (decimal adjusted). This field represents the aggregate fees paid by users for transaction processing during the hour. Used for fee revenue analysis, network economics monitoring, and understanding transaction cost patterns. Example: 1250.75 for 1,250.75 FLOW in total fees collected. Critical for network economics analysis, validator reward calculations, and understanding the cost of blockchain operations.

{% enddocs %}

{% docs total_fees_usd %}

The total sum of all transaction fees collected within the specified hour period, converted to USD equivalent value. Data type: NUMBER (decimal adjusted). This field represents the aggregate fees paid by users for transaction processing during the hour, normalized to USD for cross-currency comparison and financial analysis. Used for fee revenue analysis, network economics monitoring, and understanding transaction cost patterns in standardized currency terms. Example: 1250.50 for $1,250.50 USD equivalent in total fees collected. Critical for network economics analysis, financial reporting, and understanding the real-world cost of blockchain operations.

{% enddocs %}
