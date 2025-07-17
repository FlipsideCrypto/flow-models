{% docs beta__dim_nfl_teams %}
# beta__dim_nfl_teams

## Description

A beta-dimensional table containing NFL team metadata from the ESPN Teams Endpoint. This table serves as an experimental data source for NFL AllDay NFT analytics and team-related analysis. The table is in beta mode and subject to change, providing team information that may be used for NFT moment analysis, team performance tracking, and sports analytics. Due to its experimental nature, this table should not be used for production purposes without accepting the risk of sudden changes or deletion.

## Key Use Cases

- **NFL AllDay Analytics**: Supporting NFT moment analysis and team-related NFT performance tracking
- **Team Research**: Providing team metadata for sports analytics and team performance analysis
- **Experimental Features**: Supporting beta features and experimental analytics in the sports NFT space
- **Data Exploration**: Investigating potential use cases for team data in blockchain applications
- **Prototype Development**: Supporting prototype development for sports-related blockchain features

## Important Relationships

- **beta__dim_nfl_athletes**: Links to athlete information for team-athlete relationship analysis
- **beta__dim_nflad_playoff_rosters**: May link to playoff roster information for seasonal analysis
- **nft__dim_moment_metadata**: May overlap with NFL AllDay NFT moment data
- **beta__ez_moment_player_ids**: May provide additional team identification and linking capabilities

## Commonly-used Fields

- **Team identification fields**: For team identification and analysis
- **League association fields**: For team-league relationship analysis
- **Metadata fields**: For team information and categorization 
{% enddocs %} 