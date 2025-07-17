{% docs price__ez_prices_hourly %}
# price__ez_prices_hourly

## Description

A curated and deduplicated hourly price table that provides a single source of truth for asset prices across different blockchain networks. This table consolidates price data from multiple providers, resolving conflicts and maintaining one price per unique asset per hour. It serves as the primary price table for analytics, reporting, and portfolio management, providing clean, reliable price data that prioritizes data quality and consistency. The table includes comprehensive asset metadata for easy analysis and reporting.

## Key Use Cases

- **Portfolio Valuation**: Calculating current and historical portfolio values across multiple assets
- **Price Analysis**: Conducting price trend analysis and market performance evaluation
- **Cross-Chain Analytics**: Comparing asset performance across different blockchain networks
- **Reporting and Dashboards**: Providing clean price data for user interfaces and automated reporting
- **Risk Management**: Monitoring price movements and calculating risk metrics
- **Trading Operations**: Supporting trading decisions with reliable price data

## Important Relationships

- **price__fact_prices_ohlc_hourly**: Raw source data that feeds into this curated table
- **price__ez_asset_metadata**: Provides comprehensive asset metadata for analysis
- **price__dim_asset_metadata**: Raw asset metadata source for additional provider-specific information
- **core__fact_transactions**: For transaction value analysis and price impact correlation studies
- **defi__fact_dex_swaps**: For DeFi trading analysis and price impact on swap volumes

## Commonly-used Fields

- **HOUR**: Essential for time-series analysis and temporal data aggregation
- **TOKEN_ADDRESS**: Primary identifier for blockchain-specific operations and smart contract interactions
- **SYMBOL**: Most commonly used field for asset identification in reports and dashboards
- **PRICE**: Core price field used for valuation, analysis, and reporting
- **BLOCKCHAIN**: Essential for cross-chain analysis and blockchain-specific filtering
- **IS_IMPUTED**: Important for data quality assessment and filtering potentially unreliable prices
{% enddocs %} 