{% docs rewards__fact_points_balances %}
## Description
User points balances for the rewards system. This table tracks current point balances for users, enabling analysis of reward accumulation, spending patterns, and user engagement with the rewards ecosystem.

## Key Use Cases
- User balance tracking and analysis
- Reward accumulation monitoring
- Spending pattern analysis
- User engagement metrics

## Important Relationships
- Links to `rewards__fact_points_transfers` for balance changes
- Connects to `rewards__fact_transaction_entries` for detailed transaction history
- Supports rewards system analytics

## Commonly-used Fields
- `user_id`: User identifier
- `points_balance`: Current point balance
- `inserted_timestamp`: When the balance was recorded
{% enddocs %} 