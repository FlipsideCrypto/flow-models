{% docs evm_contracts_table_doc %}

## Description

A dimensional table containing metadata for smart contracts deployed on EVM-compatible blockchains. This table serves as the foundation for contract identification and analysis, providing information about contract addresses, names, symbols, decimals, and creation details. The table supports contract analysis, token identification, and smart contract ecosystem understanding across multiple EVM chains. Each record represents a unique contract with comprehensive metadata and deployment information.

## Key Use Cases

- **Contract Identification**: Identifying and categorizing smart contracts across EVM chains
- **Token Analysis**: Understanding token contracts, their properties, and metadata
- **Contract Creation Tracking**: Monitoring new contract deployments and creation patterns
- **Contract Ecosystem Analysis**: Understanding the distribution and types of contracts
- **Cross-Chain Contract Analysis**: Comparing contracts across different EVM-compatible blockchains
- **Contract Attribution**: Identifying contract creators and deployment patterns

## Important Relationships

- **core_evm__fact_transactions**: Links to transactions involving these contracts
- **core_evm__fact_event_logs**: Links to events emitted by these contracts
- **core_evm__fact_traces**: Links to execution traces involving these contracts
- **core_evm__dim_labels**: Links to contract labels for additional categorization
- **core_evm__ez_token_transfers**: Links to token transfers involving these contracts
- **core__dim_contract_labels**: May provide comparison data with native Flow contracts

## Commonly-used Fields

- **ADDRESS**: Essential for contract identification and cross-table joins
- **NAME**: Important for contract identification and user interface display
- **SYMBOL**: Critical for token contracts and trading interface support
- **DECIMALS**: Key for token amount calculations and precision handling
- **CREATED_BLOCK_NUMBER**: Important for contract creation tracking and temporal analysis
- **CREATOR_ADDRESS**: Critical for contract attribution and creator analysis

{% enddocs %}

{% docs evm_contracts_block_number %}

The block number at which the contract was read and details recorded.

{% enddocs %}

{% docs evm_contracts_block_time %}

The block timestamp at which the contract was read and details recorded.

{% enddocs %}

{% docs evm_contracts_contract_address %}

The unique address of the deployed contract.

{% enddocs %}

{% docs evm_contracts_metadata %}

This JSON column contains other relevant details for each contract.

{% enddocs %}

{% docs evm_contracts_name %}

The name of the deployed contract. Please note this is not necessarily unique.

{% enddocs %}

{% docs evm_contracts_symbol %}

The symbol used to represent this contract. Please note this is not necessarily unique.

{% enddocs %}

{% docs evm_creator_address %}

The address of the contract creator.

{% enddocs %}

{% docs evm_logic_address %}

The logic address, where applicable.

{% enddocs %}

{% docs evm_system_created_at %}

Internal column.

{% enddocs %}

{% docs evm_token_convention %}

The token standard utilized by this contract.

{% enddocs %}

{% docs evm_decimals %}

The decimals for the token contract.

{% enddocs %}

{% docs evm_contracts_created_tx_hash %}

The transaction hash at which the contract was created.

{% enddocs %}