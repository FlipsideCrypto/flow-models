{% docs price__ez_asset_metadata %}
# price__ez_asset_metadata

## Description

A curated and deduplicated asset metadata table that provides a single source of truth for asset information across different blockchain networks. This table consolidates data from multiple price providers, resolving conflicts and maintaining one record per unique asset per blockchain. It serves as the primary dimension table for asset identification in price analytics and provides clean, reliable metadata for reporting and analysis. The table prioritizes data quality and consistency over raw data preservation.

## Key Use Cases

- **Asset Reference**: Primary lookup table for asset metadata in price analytics and reporting
- **Portfolio Management**: Providing consistent asset information for multi-asset portfolio tracking
- **Price Analysis**: Joining with price fact tables for comprehensive asset price analysis
- **Cross-Chain Analytics**: Understanding asset distribution and characteristics across blockchain networks
- **Reporting and Dashboards**: Providing clean, consistent asset names and symbols for user interfaces
- **Data Quality Assurance**: Ensuring consistent asset representation across different data sources

## Important Relationships

- **price__dim_asset_metadata**: Raw source data that feeds into this curated table
- **price__fact_prices_ohlc_hourly**: Links to raw price data through asset_id
- **price__ez_prices_hourly**: Primary price table that uses this for asset metadata
- **core__dim_contract_labels**: Provides additional labeling for contract-based assets
- **evm/core_evm__dim_contracts**: For EVM tokens, provides additional contract-level metadata

## Commonly-used Fields

- **TOKEN_ADDRESS**: Primary identifier for blockchain-specific operations and smart contract interactions
- **SYMBOL**: Most commonly used field for asset identification in reports and dashboards
- **NAME**: Human-readable asset name for display and user interfaces
- **BLOCKCHAIN**: Essential for cross-chain analysis and blockchain-specific filtering
- **DECIMALS**: Critical for accurate price calculations and token amount conversions
- **IS_NATIVE**: Important for distinguishing native blockchain tokens from smart contract tokens
{% enddocs %} 