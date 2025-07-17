# rewards__fact_points_balances

## Description

A fact table containing point-in-time balances for user reward accounts from the Reward Points API. This table provides snapshots of user balances including points, boxes, and keys at specific request dates. The data represents balance states rather than transaction flows, with early data potentially being patchy due to API limitations. The table supports reward program analytics by providing current and historical balance information for user engagement analysis and program performance tracking.

## Key Use Cases

- **Balance Tracking**: Monitoring current and historical user reward balances across different asset types
- **User Engagement Analysis**: Understanding user participation levels and reward accumulation patterns
- **Program Performance**: Analyzing reward program effectiveness and user retention
- **Balance Reconciliation**: Verifying account balances and identifying data quality issues
- **Reward Distribution Analysis**: Understanding how rewards are distributed across the user base
- **Predictive Analytics**: Using balance patterns to predict user behavior and program outcomes

## Important Relationships

- **rewards__fact_points_transfers**: Provides transaction history that explains balance changes
- **rewards__fact_transaction_entries**: Offers detailed transaction-level data for balance reconciliation
- **rewards__dim_storefront_items**: Links to items that can be purchased with accumulated points
- **core__dim_address_mapping**: May link to user addresses for additional user context
- **evm/core_evm__dim_contracts**: For contract-based reward systems, provides contract metadata

## Commonly-used Fields

- **ADDRESS**: Primary identifier for user accounts and balance tracking
- **POINTS**: Core reward currency balance for program analysis
- **BOXES**: Special reward item balance for gamification analysis
- **KEYS**: Special reward item balance for access control analysis
- **REQUEST_DATE**: Temporal reference for balance snapshots and time-series analysis
- **BOXES_OPENED**: Historical metric for user engagement and reward consumption 