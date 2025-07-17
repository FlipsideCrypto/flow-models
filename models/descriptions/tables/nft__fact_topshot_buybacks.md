{% docs nft__fact_topshot_buybacks %}
## Description
Records all NBA TopShot buyback transactions where the official TopShot buyback wallet purchases moments from users. This table combines sales data from both the standard TopShot marketplace and Flowty, tracking the buyback program's activity with running totals of amounts spent over time. The table includes enriched metadata from both TopShot and on-chain sources, providing comprehensive tracking of the buyback program's market impact and spending patterns.

## Key Use Cases
- Track TopShot buyback program spending and activity
- Analyze buyback market impact on moment prices
- Monitor buyback wallet behavior and patterns
- Support TopShot market analysis and reporting
- Enable buyback program performance analytics

## Important Relationships
- Sources data from `nft__ez_nft_sales` for marketplace transactions
- Joins with `nft__dim_topshot_metadata` for enriched moment data
- Links to `nft__dim_moment_metadata` for on-chain metadata
- Filters for specific buyback wallet address (0xe1f2a091f7bb5245)

## Commonly-used Fields
- `tx_id`: Unique transaction identifier for buyback event
- `block_timestamp`: When the buyback transaction occurred
- `nft_id`: TopShot moment identifier
- `player`: Athlete featured in the bought moment
- `team`: Team affiliation at time of moment
- `price`: USD price paid for the moment
- `total`: Running total of all buyback purchases
- `buyer`: TopShot buyback wallet address
- `seller`: Original moment owner

{% enddocs %} 