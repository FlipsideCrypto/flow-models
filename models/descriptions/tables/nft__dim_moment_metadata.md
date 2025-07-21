{% docs nft__dim_moment_metadata %}
## Description
NFT moment metadata scraped from on-chain activity across multiple Flow NFT projects. This table captures metadata that is stored directly on the blockchain when moments are minted, providing a decentralized source of NFT information. The table supports multiple NFT collections including All Day, TopShot, and Golazos, with comprehensive metadata including play details, series information, and collection hierarchy.

## Key Use Cases
- Analyze on-chain NFT metadata across multiple collections
- Track moment creation and minting activity
- Support cross-collection NFT analytics
- Enable decentralized metadata verification
- Power NFT explorer and discovery tools

## Important Relationships
- Can be joined with `nft__ez_nft_sales` for sales analytics
- Links to `nft__dim_allday_metadata` and `nft__dim_topshot_metadata` for enriched data
- NFT collection and ID enable cross-table joins
- Supports multiple NFT projects on Flow

## Commonly-used Fields
- `nft_collection`: Contract address of the NFT collection
- `nft_id`: Unique identifier for the NFT moment
- `serial_number`: Edition number within the collection
- `set_name`: Name of the set containing the moment
- `series_name`: Series name for organizational hierarchy
- `metadata`: JSON object containing detailed moment information
- `tier`: Rarity tier classification

{% enddocs %} 