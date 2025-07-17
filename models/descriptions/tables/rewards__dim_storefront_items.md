{% docs rewards__dim_storefront_items %}
## Description
Storefront items and metadata for the rewards system. This table contains information about items available in the rewards storefront, including pricing, availability, and item details for reward redemption analysis.

## Key Use Cases
- Storefront item management and analysis
- Reward redemption tracking
- Item availability monitoring
- Storefront performance analysis

## Important Relationships
- Links to `rewards__fact_points_transfers` for redemption transactions
- Connects to `rewards__fact_points_balances` for user balance analysis
- Supports rewards system analytics

## Commonly-used Fields
- `store_id`: Store identifier
- `item_id`: Unique item identifier
- `item_name`: Human-readable item name
- `item_price`: Cost in reward points
- `item_available`: Availability status
{% enddocs %} 