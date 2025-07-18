{% docs nft__dim_allday_metadata %}
## Description
Comprehensive metadata for NFL All Day Moments, including player information, team details, statistics, and rich content data. This table is produced via API integration and provides detailed information about each NFL All Day NFT moment, including play descriptions, video URLs, and comprehensive statistics. The data structure may differ from on-chain metadata available in other tables, providing a more complete and curated view of NFL All Day moments.

## Key Use Cases
- Analyze NFL All Day moment performance and market activity
- Track player and team-based NFT analytics
- Power NFL All Day dashboards and user-facing applications
- Support content discovery and moment exploration
- Enable player and team-based market analysis

## Important Relationships
- Can be joined with `nft__ez_nft_sales` for sales analytics
- Links to `nft__dim_moment_metadata` for on-chain data comparison
- Player and team data enables cross-platform athlete analysis
- NFL All Day ID enables direct marketplace integration

## Commonly-used Fields
- `nft_id`: Unique identifier for the NFL All Day moment
- `nflallday_id`: Official NFL All Day identifier for marketplace integration
- `player`: Athlete featured in the moment
- `team`: Team affiliation at time of moment
- `season`: NFL season when moment occurred
- `set_name`: Collection set name for organization
- `total_circulation`: Maximum supply for rarity analysis
- `moment_description`: Detailed play description

{% enddocs %} 