{% docs price__fact_prices_ohlc_hourly %}
# price__fact_prices_ohlc_hourly

## Description

A comprehensive fact table containing raw OHLC (Open, High, Low, Close) price data at hourly intervals from multiple price providers. This table preserves the original provider structure and contains uncurated price data that may include duplicates, inconsistencies, and data quality issues inherent to the source APIs. It serves as the foundation for price analysis while maintaining data lineage and enabling provider-specific analysis. The table supports detailed price movement analysis and technical indicator calculations.

## Key Use Cases

- **Technical Analysis**: Calculating technical indicators, support/resistance levels, and price patterns
- **Volatility Analysis**: Measuring price volatility and risk metrics across different time periods
- **Provider Comparison**: Analyzing price differences and data quality across different price providers
- **Data Quality Assessment**: Identifying gaps, anomalies, and inconsistencies in price data
- **Raw Data Access**: Providing access to unprocessed price data for custom analysis and backtesting
- **Historical Price Analysis**: Conducting detailed historical price movement analysis and research

## Important Relationships

- **price__dim_asset_metadata**: Links to asset metadata through asset_id and provider
- **price__ez_asset_metadata**: Provides curated asset information for analysis
- **price__ez_prices_hourly**: Curated, deduplicated version of this table with one price per asset per hour
- **core__fact_blocks**: May be used for blockchain-specific price analysis and correlation studies
- **core__fact_transactions**: For transaction value analysis and price impact studies

## Commonly-used Fields

- **HOUR**: Essential for time-series analysis and temporal data aggregation
- **ASSET_ID**: Primary identifier for joining with asset metadata and other price-related data
- **OPEN**: Starting price for the hour, used in OHLC analysis and trend calculations
- **HIGH**: Maximum price during the hour, important for resistance level analysis
- **LOW**: Minimum price during the hour, important for support level analysis
- **CLOSE**: Ending price for the hour, most commonly used for price trend analysis
{% enddocs %} 