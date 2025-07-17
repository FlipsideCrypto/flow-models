{% docs price__ez_prices_hourly %}
## Description
Easy-to-use hourly price data table providing aggregated and cleaned price information for assets across multiple blockchains. This table presents hourly price snapshots with business-friendly formatting and includes metadata about data quality and imputation.

## Key Use Cases
- Hourly price analysis and reporting
- Asset performance tracking
- Portfolio valuation
- Price trend analysis

## Important Relationships
- Links to `price__ez_asset_metadata` for asset information
- Connects to `price__fact_prices_ohlc_hourly` for OHLC data
- Supports price-based analytics

## Commonly-used Fields
- `prices_hour`: Hourly timestamp for time-series analysis
- `prices_token_address`: Asset contract address
- `prices_price`: Hourly price value
- `prices_symbol`: Asset symbol for identification
- `prices_is_imputed`: Data quality indicator
{% enddocs %} 