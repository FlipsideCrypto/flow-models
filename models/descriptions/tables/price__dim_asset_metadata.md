{% docs price__dim_asset_metadata %}
# price__dim_asset_metadata

## Description

A comprehensive dimensional table containing raw asset metadata from multiple price providers. This table serves as the foundation for asset identification and metadata management across different blockchain networks and price data sources. It contains uncurated, provider-specific data that may include duplicates, inconsistencies, and data quality issues inherent to the source APIs. The table maintains the original provider structure to preserve data lineage and enable source-specific analysis.

## Key Use Cases

- **Asset Discovery and Identification**: Finding all available assets across different price providers and blockchain networks
- **Data Quality Analysis**: Identifying inconsistencies, duplicates, and gaps in asset metadata across providers
- **Provider Comparison**: Analyzing how different price providers categorize and describe the same assets
- **Cross-Chain Asset Mapping**: Understanding how assets are represented across different blockchain networks
- **Data Lineage Tracking**: Maintaining audit trails for asset metadata changes and provider updates
- **Raw Data Access**: Providing access to unprocessed asset metadata for custom curation and analysis

## Important Relationships

- **price__ez_asset_metadata**: Curated, deduplicated version of this table with one record per unique asset
- **price__fact_prices_ohlc_hourly**: Links to price data through asset_id and provider
- **price__ez_prices_hourly**: Provides curated price data for assets in this dimension
- **core__dim_contract_labels**: May overlap with contract-based assets for additional labeling
- **evm/core_evm__dim_contracts**: For EVM-based tokens, provides additional contract metadata

## Commonly-used Fields

- **PROVIDER**: Essential for filtering by data source and understanding data provenance
- **ASSET_ID**: Primary identifier for joining with price fact tables and other asset-related data
- **TOKEN_ADDRESS**: Critical for blockchain-specific operations and smart contract interactions
- **BLOCKCHAIN**: Key for cross-chain analysis and blockchain-specific filtering
- **SYMBOL**: Commonly used for asset identification in reports and dashboards
- **NAME**: Human-readable asset name for display and reporting purposes
{% enddocs %} 