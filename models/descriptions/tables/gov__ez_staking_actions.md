{% docs gov__ez_staking_actions %}
## Description
Easy-to-use staking actions table providing simplified access to governance staking activities. This table aggregates staking transactions and provides business-friendly views of delegation, undelegation, and staking operations across the governance system.

## Key Use Cases
- Staking activity analysis and reporting
- Delegation tracking and monitoring
- Governance participation analysis
- Validator performance correlation

## Important Relationships
- Links to `gov__dim_validator_labels` for validator information
- Connects to `core__fact_transactions` for transaction details
- Supports governance analytics

## Commonly-used Fields
- `tx_id`: Transaction identifier
- `delegator`: Address performing the staking action
- `action`: Type of staking action (delegate, undelegate, etc.)
- `amount`: Amount being staked or unstaked
- `node_id`: Target validator node
{% enddocs %} 