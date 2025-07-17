{% docs price__ez_asset_metadata %}
## Description
Easy-to-use curated asset metadata table providing deduplicated and cleaned asset information. This table aggregates asset metadata from multiple providers and presents a unified view of assets across different blockchains with business-friendly formatting.

## Key Use Cases
- Asset lookup and identification
- Cross-blockchain asset analysis
- Price data correlation
- Asset reporting and dashboards

## Important Relationships
- Links to `price__dim_asset_metadata` for raw provider data
- Connects to `price__ez_prices_hourly` for price information
- Supports asset-based analytics

## Commonly-used Fields
- `prices_asset_id`: Unique asset identifier
- `prices_symbol`: Asset symbol for identification
- `prices_token_address`: Contract address for tokens
- `prices_blockchain`: Blockchain where asset exists
{% enddocs %} 