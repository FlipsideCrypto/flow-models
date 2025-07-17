{% docs rewards__fact_points_transfers %}
# rewards__fact_points_transfers

## Description

A fact table containing reward points transfer transactions from the Reward Points API Transfers Endpoint. This table tracks the movement of points, boxes, and keys between user accounts in batch transfer operations. Each transfer is organized within batches and includes detailed metadata about the sending and receiving accounts. The table provides the transaction-level foundation for understanding reward flows, user interactions, and program participation patterns.

## Key Use Cases

- **Transfer Tracking**: Monitoring reward point movements between user accounts and system operations
- **Batch Analysis**: Understanding transfer batch operations and their impact on the reward system
- **User Interaction Analysis**: Identifying patterns in how users transfer rewards to each other
- **Program Participation**: Measuring user engagement through transfer activity
- **Audit and Compliance**: Maintaining detailed records of all reward transfers for regulatory and internal audit purposes
- **Network Analysis**: Understanding reward flow patterns and identifying key users in the reward ecosystem

## Important Relationships

- **rewards__fact_points_balances**: Links to account balances that are affected by these transfers
- **rewards__fact_transaction_entries**: Provides more granular transaction details for transfer analysis
- **rewards__dim_storefront_items**: May link to items purchased with transferred points
- **core__dim_address_mapping**: Provides additional user context for transfer analysis
- **evm/core_evm__dim_contracts**: For contract-based transfers, provides contract metadata

## Commonly-used Fields

- **BATCH_ID**: Primary identifier for grouping related transfers in batch operations
- **FROM_ADDRESS**: Source account for transfer tracking and user behavior analysis
- **TO_ADDRESS**: Destination account for transfer tracking and user behavior analysis
- **POINTS**: Core reward currency transferred for value analysis
- **BOXES**: Special reward items transferred for gamification analysis
- **KEYS**: Special reward items transferred for access control analysis
- **CREATED_AT**: Temporal reference for transfer timing and chronological analysis
{% enddocs %} 