{% docs rewards__fact_transaction_entries %}
## Description
Detailed transaction entries for the rewards system. This table provides granular transaction-level data including balance changes, direction, and amounts for comprehensive rewards system analysis.

## Key Use Cases
- Detailed transaction analysis
- Balance change tracking
- User behavior analysis
- System audit and reconciliation

## Important Relationships
- Links to `rewards__fact_points_balances` for balance impact
- Connects to `rewards__fact_points_transfers` for transfer context
- Supports detailed rewards analytics

## Commonly-used Fields
- `entry_id`: Unique entry identifier
- `direction`: Transaction direction (in/out)
- `amount`: Transaction amount
- `account_id`: Account identifier
- `user_id`: User identifier
{% enddocs %} 