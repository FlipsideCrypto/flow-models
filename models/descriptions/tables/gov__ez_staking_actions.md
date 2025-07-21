{% docs gov__ez_staking_actions %}
# gov__ez_staking_actions

## Description

A fact table containing transaction-level information on FLOW staking activities within the governance system. This table tracks all staking-related actions including delegation, undelegation, and reward collection operations. Each record represents a specific staking action with detailed metadata about the delegator, validator node, action type, and amount involved. The table serves as the primary source for staking analytics and governance participation analysis in the Flow ecosystem.

## Key Use Cases

- **Staking Analysis**: Tracking delegation and undelegation patterns across the Flow network
- **Governance Participation**: Understanding user engagement in the staking ecosystem
- **Validator Performance**: Analyzing validator attractiveness and delegation trends
- **Reward Distribution**: Monitoring staking rewards and their distribution patterns
- **Network Security**: Understanding the distribution of staked FLOW and its impact on network security
- **User Behavior Analysis**: Identifying patterns in how users interact with the staking system

## Important Relationships

- **gov__dim_validator_labels**: Links to validator metadata for node analysis and categorization
- **core__fact_transactions**: Provides additional transaction context and blockchain data
- **core__fact_events**: May link to staking-related events for detailed governance analysis
- **core__dim_address_mapping**: Provides additional user context for delegator analysis
- **price__ez_prices_hourly**: May link to FLOW price data for staking value analysis

## Commonly-used Fields

- **TX_ID**: Primary identifier for linking to blockchain transactions and audit trails
- **DELEGATOR**: User account address for delegator identification and behavior analysis
- **NODE_ID**: Validator node identifier for validator-specific analysis
- **ACTION**: Staking operation type for transaction categorization and analysis
- **AMOUNT**: Staking amount for value analysis and volume tracking
- **BLOCK_TIMESTAMP**: Temporal reference for chronological analysis and time-series tracking
{% enddocs %} 