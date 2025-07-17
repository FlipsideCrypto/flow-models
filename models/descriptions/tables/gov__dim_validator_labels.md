{% docs gov__dim_validator_labels %}
## Description
Governance validator labels and metadata for staking and delegation analysis. This table provides human-readable labels and categorization for validators and nodes in the governance system, enabling easier analysis of staking activities and validator performance.

## Key Use Cases
- Validator identification and categorization
- Staking and delegation analysis
- Governance participation tracking
- Validator performance monitoring

## Important Relationships
- Links to `gov__ez_staking_actions` for staking activity analysis
- Supports validator-based joins across governance tables

## Commonly-used Fields
- `node_id`: The validator node identifier
- `validator_type`: Type of validator (consensus, etc.)
- `project_name`: Associated project or organization
{% enddocs %} 