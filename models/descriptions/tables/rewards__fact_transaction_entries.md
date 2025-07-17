# rewards__fact_transaction_entries

## Description

A fact table containing individual Flow points transfer events flattened from responses from the Snag transaction entries endpoint. This table provides the most granular level of transaction detail in the rewards system, tracking individual entry-level transactions with balance state information. Each record represents a single transaction entry with before and after balance states, enabling detailed audit trails and transaction-level analysis. The table serves as the foundation for understanding reward system operations at the most detailed level.

## Key Use Cases

- **Transaction Analysis**: Examining individual reward transactions for detailed audit and analysis
- **Balance Reconciliation**: Verifying account balance changes through detailed transaction records
- **Audit Trails**: Maintaining comprehensive records of all reward system activities
- **User Behavior Analysis**: Understanding detailed patterns in how users interact with the reward system
- **System Performance**: Monitoring transaction processing and identifying potential issues
- **Compliance Reporting**: Providing detailed transaction records for regulatory and internal compliance

## Important Relationships

- **rewards__fact_points_balances**: Links to account balances that are affected by these transactions
- **rewards__fact_points_transfers**: Provides batch-level context for individual transaction entries
- **rewards__dim_storefront_items**: May link to items involved in transaction entries
- **core__fact_transactions**: May link to underlying blockchain transactions for additional context
- **core__dim_address_mapping**: Provides user context for transaction analysis

## Commonly-used Fields

- **ENTRY_ID**: Primary identifier for individual transaction entries and audit trails
- **ACCOUNT_ID**: User account identifier for transaction attribution and analysis
- **USER_WALLET_ADDRESS**: Blockchain address for cross-system analysis and user identification
- **DIRECTION**: Transaction direction (credit/debit) for understanding reward flows
- **AMOUNT**: Transaction value for financial analysis and volume tracking
- **AMOUNT_START/AMOUNT_END**: Balance state information for transaction validation and audit
- **CREATED_AT**: Temporal reference for transaction timing and chronological analysis 