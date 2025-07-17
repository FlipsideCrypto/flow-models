{% docs rewards__fact_points_transfers %}
## Description
Points transfer transactions in the rewards system. This table tracks all point transfers including earning, spending, and redemption activities, providing a complete audit trail of reward point movements.

## Key Use Cases
- Transfer tracking and analysis
- Reward earning/spending patterns
- Redemption activity monitoring
- Audit trail and compliance

## Important Relationships
- Links to `rewards__fact_points_balances` for balance impact
- Connects to `rewards__fact_transaction_entries` for detailed transaction data
- Supports rewards system analytics

## Commonly-used Fields
- `batch_id`: Transfer batch identifier
- `from_address`: Source address
- `to_address`: Destination address
- `points`: Transfer amount
- `created_at`: Transfer timestamp
{% enddocs %} 