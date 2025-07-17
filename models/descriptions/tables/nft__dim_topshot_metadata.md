{% docs nft__dim_topshot_metadata %}
## Description
Comprehensive metadata for NBA TopShot Moments, including player information, team details, statistics, and rich content data. This table is produced via API integration and provides detailed information about each NBA TopShot NFT moment, including play descriptions, video URLs, and comprehensive player statistics. The data structure may differ from on-chain metadata, providing a more complete and curated view of NBA TopShot moments with enhanced statistical information.

## Key Use Cases
- Analyze NBA TopShot moment performance and market activity
- Track player and team-based NFT analytics
- Power NBA TopShot dashboards and user-facing applications
- Support content discovery and moment exploration
- Enable player performance correlation with NFT values

## Important Relationships
- Can be joined with `nft__ez_nft_sales` for sales analytics
- Links to `nft__dim_moment_metadata` for on-chain data comparison
- Player and team data enables cross-platform athlete analysis
- NBA TopShot ID enables direct marketplace integration
- Used by `nft__fact_topshot_buybacks` for buyback analytics

## Commonly-used Fields
- `nft_id`: Unique identifier for the NBA TopShot moment
- `nbatopshot_id`: Official NBA TopShot identifier for marketplace integration
- `player`: Athlete featured in the moment
- `team`: Team affiliation at time of moment
- `season`: NBA season when moment occurred
- `set_name`: Collection set name for organization
- `total_circulation`: Maximum supply for rarity analysis
- `moment_description`: Detailed play description
- `player_stats_game`: Game-specific player statistics

{% enddocs %} 