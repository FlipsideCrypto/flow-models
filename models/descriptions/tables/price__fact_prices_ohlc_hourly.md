{% docs price__fact_prices_ohlc_hourly %}
## Description
Raw OHLC (Open, High, Low, Close) price data at hourly intervals for assets across multiple blockchains. This table contains the foundational price data used for technical analysis, volatility calculations, and price trend analysis.

## Key Use Cases
- Technical analysis and charting
- Volatility calculations
- Price range analysis
- Market microstructure analysis

## Important Relationships
- Links to `price__ez_prices_hourly` for aggregated price data
- Connects to `price__ez_asset_metadata` for asset information
- Supports advanced price analytics

## Commonly-used Fields
- `prices_hour`: Hourly timestamp for time-series analysis
- `prices_asset_id`: Asset identifier
- `prices_open`: Opening price for the hour
- `prices_high`: Highest price during the hour
- `prices_low`: Lowest price during the hour
- `prices_close`: Closing price for the hour
{% enddocs %} 