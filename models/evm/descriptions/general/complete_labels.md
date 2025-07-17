{% docs evm_labels_table %}

This table contains labels for addresses on this EVM blockchain. 

{% enddocs %}


{% docs evm_table_dim_labels %}

## Description

A dimensional table containing address labels and categorization data for EVM-compatible blockchains. This table serves as the foundation for address identification and categorization, providing human-readable names and labels for addresses including contracts, users, and protocols. The table supports address analysis, user identification, and ecosystem understanding across multiple EVM chains. Each record represents a labeled address with comprehensive categorization and metadata.

## Key Use Cases

- **Address Identification**: Providing human-readable names for blockchain addresses
- **User Analysis**: Identifying and categorizing user addresses and their activities
- **Protocol Analysis**: Understanding protocol addresses and their roles in the ecosystem
- **Contract Categorization**: Labeling and categorizing smart contract addresses
- **Cross-Chain Labeling**: Maintaining consistent labels across different EVM-compatible blockchains
- **Ecosystem Mapping**: Understanding the relationships and roles of different addresses

## Important Relationships

- **core_evm__fact_transactions**: Links to transactions involving labeled addresses
- **core_evm__fact_event_logs**: Links to events involving labeled addresses
- **core_evm__dim_contracts**: Links to contract metadata for labeled contract addresses
- **core_evm__ez_token_transfers**: Links to token transfers involving labeled addresses
- **core_evm__ez_native_transfers**: Links to native transfers involving labeled addresses
- **core__dim_labels**: May provide comparison data with native Flow labels

## Commonly-used Fields

- **ADDRESS**: Essential for address identification and cross-table joins
- **ADDRESS_NAME**: Critical for human-readable address identification
- **LABEL_TYPE**: Important for address categorization and filtering
- **LABEL_SUBTYPE**: Key for detailed address categorization and analysis
- **LABEL**: Essential for address labeling and user interface display
- **BLOCKCHAIN**: Critical for cross-chain analysis and chain-specific labeling

{% enddocs %}

{% docs evm_project_name %}

The name of the project for this address. 

{% enddocs %}


{% docs evm_label %}

The label for this address. 

{% enddocs %}


{% docs evm_label_address %}

Address that the label is for. This is the field that should be used to join other tables with labels. 

{% enddocs %}


{% docs evm_label_address_name %}

The most granular label for this address.  

{% enddocs %}


{% docs evm_label_blockchain %}

The name of the blockchain.

{% enddocs %}


{% docs evm_label_creator %}

The name of the creator of the label.

{% enddocs %}


{% docs evm_label_subtype %}

A sub-category nested within label type providing further detail.

{% enddocs %}


{% docs evm_label_type %}

A high-level category describing the address's main function or ownership.

{% enddocs %}


