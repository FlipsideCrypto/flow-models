{% docs evm_tx_table_doc %}

## Description

A comprehensive fact table containing transaction-level data for EVM-compatible blockchains. This table serves as the primary source for transaction analysis, providing detailed information about each transaction including sender/receiver addresses, values, gas usage, fees, and execution status. The table supports analysis of transaction patterns, user behavior, and network activity across multiple EVM chains. Each record represents a single transaction with complete execution details and metadata.

## Key Use Cases

- **Transaction Analysis**: Tracking transaction volumes, success rates, and execution patterns
- **User Behavior Analysis**: Understanding transaction patterns and user activity across EVM chains
- **Gas Analysis**: Monitoring gas usage, fee structures, and transaction economics
- **Cross-Chain Activity**: Comparing transaction patterns across different EVM-compatible blockchains
- **Contract Interaction Analysis**: Understanding smart contract usage and interaction patterns
- **Network Performance**: Monitoring transaction processing efficiency and network health

## Important Relationships

- **core_evm__fact_blocks**: Links to block data containing each transaction
- **core_evm__fact_event_logs**: Links to event logs emitted by transactions
- **core_evm__fact_traces**: Links to execution traces for detailed transaction analysis
- **core_evm__dim_contracts**: Links to contract metadata for contract interactions
- **core_evm__dim_labels**: Links to address labels for user identification
- **core__fact_transactions**: May provide comparison data with native Flow transactions

## Commonly-used Fields

- **TX_HASH**: Essential for transaction identification and blockchain verification
- **FROM_ADDRESS/TO_ADDRESS**: Critical for transaction flow analysis and user behavior studies
- **VALUE**: Important for transaction value analysis and economic activity tracking
- **GAS_USED/GAS_PRICE**: Key for gas economics and transaction cost analysis
- **TX_SUCCEEDED**: Critical for success rate analysis and error tracking
- **BLOCK_TIMESTAMP**: Essential for time-series analysis and temporal data aggregation

{% enddocs %}


{% docs evm_cumulative_gas_used %}

The total amount of gas used when this transaction was executed in the block. 

{% enddocs %}


{% docs evm_tx_block_hash %}

Block hash is a unique 66-character identifier that is generated when a block is produced. 

{% enddocs %}


{% docs evm_tx_fee %}

Amount paid to validate the transaction in the native asset. 

{% enddocs %}


{% docs evm_tx_gas_limit %}

Maximum amount of gas allocated for the transaction. 

{% enddocs %}


{% docs evm_tx_gas_price %}

Cost per unit of gas in Gwei. 

{% enddocs %}


{% docs evm_tx_gas_used %}

Gas used by the transaction.

{% enddocs %}


{% docs evm_tx_hash %}

Transaction hash is a unique 66-character identifier that is generated when a transaction is executed. 

{% enddocs %}


{% docs evm_tx_input_data %}

This column contains additional data for this transaction, and is commonly used as part of a contract interaction or as a message to the recipient.  

{% enddocs %}


{% docs evm_tx_json %}

This JSON column contains the transaction details, including event logs. 

{% enddocs %}


{% docs evm_tx_nonce %}

The number of transactions sent from a given address. 

{% enddocs %}


{% docs evm_tx_origin_sig %}

The function signature of the call that triggered this transaction. 

{% enddocs %}

{% docs evm_origin_sig %}

The function signature of the contract call that triggered this transaction.

{% enddocs %}


{% docs evm_tx_position %}

The position of the transaction within the block. 

{% enddocs %}


{% docs evm_tx_status %}

Status of the transaction. 

{% enddocs %}


{% docs evm_value %}

The value transacted in the native asset. 

{% enddocs %}


{% docs evm_effective_gas_price %}

The total base charge plus tip paid for each unit of gas, in Gwei.

{% enddocs %}

{% docs evm_max_fee_per_gas %}

The maximum fee per gas of the transaction, in Gwei.

{% enddocs %}


{% docs evm_max_priority_fee_per_gas %}

The maximum priority fee per gas of the transaction, in Gwei.

{% enddocs %}


{% docs evm_r %}

The r value of the transaction signature.

{% enddocs %}


{% docs evm_s %}

The s value of the transaction signature.

{% enddocs %}


{% docs evm_v %}

The v value of the transaction signature.

{% enddocs %}

{% docs evm_tx_succeeded %}

Whether the transaction was successful, returned as a boolean.

{% enddocs %}

{% docs evm_tx_fee_precise %}

The precise amount of the transaction fee. This is returned as a string to avoid precision loss. 

{% enddocs %}

{% docs evm_tx_type %}

The type of transaction. 

{% enddocs %}

{% docs evm_mint %}

The minting event associated with the transaction

{% enddocs %}

{% docs evm_source_hash %}

The hash of the source transaction that created this transaction

{% enddocs %}

{% docs evm_eth_value %}

The eth value for the transaction

{% enddocs %}

{% docs evm_chain_id %}

The unique identifier for the chain the transaction was executed on.

{% enddocs %}

{% docs evm_l1_fee_precise_raw %}

The raw l1 fee for the transaction, in Gwei.

{% enddocs %}

{% docs evm_l1_fee_precise %}

The precise l1 fee for the transaction, in Gwei.

{% enddocs %}

{% docs evm_y_parity %}

The y parity for the transaction.

{% enddocs %}

{% docs evm_access_list %}

The access list for the transaction.

{% enddocs %}

{% docs evm_l1_base_fee_scalar %}

The scalar l1 base fee for the transaction.

{% enddocs %}

{% docs evm_l1_blob_base_fee %}

The blob base fee for the transaction.

{% enddocs %}

{% docs evm_l1_blob_base_fee_scalar %}

The scalar blob base fee for the transaction.

{% enddocs %}