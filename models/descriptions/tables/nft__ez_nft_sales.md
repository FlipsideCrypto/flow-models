{% docs nft__ez_nft_sales %}
## Description
Comprehensive view of all NFT marketplace sales on the Flow blockchain, providing a unified interface for NFT trading activity across multiple marketplaces and collections. This table captures sales from the primary NFT Marketplace contract, Fabricant NFT Marketplace, NBA TopShot primary market, and other supported marketplaces. The table includes transaction details, pricing information, and marketplace attribution, enabling cross-platform NFT sales analysis.

## Key Use Cases
- Analyze NFT sales activity across multiple marketplaces
- Track collection and marketplace performance
- Power NFT market dashboards and analytics
- Support price analysis and market trends
- Enable cross-collection and cross-marketplace comparisons

## Important Relationships
- Sources data from `silver__nft_sales_s` for unified sales data
- Can be joined with metadata tables (`nft__dim_allday_metadata`, `nft__dim_topshot_metadata`, etc.) for enriched analytics
- Links to `nft__fact_topshot_buybacks` for buyback program analysis
- Supports multiple NFT collections and marketplaces

## Commonly-used Fields
- `tx_id`: Unique transaction identifier for the sale
- `block_timestamp`: When the sale transaction occurred
- `marketplace`: Name of the marketplace where sale occurred
- `nft_collection`: Contract address of the NFT collection
- `nft_id`: Unique identifier for the NFT sold
- `buyer`: Address of the buyer
- `seller`: Address of the seller
- `price`: Sale price in the specified currency
- `currency`: Currency used for the transaction

{% enddocs %} 