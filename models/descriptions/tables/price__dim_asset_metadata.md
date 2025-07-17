{% docs price__dim_asset_metadata %}
## Description
Asset metadata and information for price data across multiple providers and blockchains. This table provides comprehensive asset information including names, symbols, addresses, and blockchain details to support price analytics and asset identification.

## Key Use Cases
- Asset identification and metadata lookup
- Cross-provider price data correlation
- Multi-blockchain asset tracking
- Price analytics and reporting

## Important Relationships
- Links to `price__ez_prices_hourly` for price data
- Connects to `price__fact_prices_ohlc_hourly` for OHLC data
- Supports asset-based joins across price tables

## Commonly-used Fields
- `prices_asset_id`: Unique asset identifier
- `prices_symbol`: Asset symbol (e.g., BTC, ETH)
- `prices_token_address`: Contract address for tokens
- `prices_blockchain`: Blockchain where asset exists
{% enddocs %} 