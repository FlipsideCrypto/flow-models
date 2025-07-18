{% docs gov__dim_validator_labels %}

## Description

A dimensional table containing validator node metadata and labeling information for the Flow blockchain network. This table serves as the foundation for understanding the validator ecosystem, providing information about node types, roles, and associated projects or organizations. It supports governance analysis by enabling identification and categorization of validator nodes based on their function in the consensus mechanism and their operational characteristics.

## Key Use Cases

- **Validator Identification**: Identifying and categorizing validator nodes by type and role in the network
- **Governance Analysis**: Understanding the distribution of validator types and their impact on network security
- **Network Architecture**: Analyzing the composition of the Flow blockchain's multi-role validator system
- **Project Attribution**: Identifying which organizations or projects operate specific validator nodes
- **Staking Analysis**: Supporting staking operations by providing validator metadata for delegation decisions
- **Network Health Monitoring**: Tracking validator distribution and identifying potential centralization risks

## Important Relationships

- **gov__ez_staking_actions**: Links to staking transactions involving these validator nodes
- **core__fact_transactions**: May link to validator-related transactions for additional context
- **core__dim_contract_labels**: May provide additional labeling for validator contracts
- **core__fact_events**: May link to validator-related events for governance analysis
- **evm/core_evm__dim_contracts**: For EVM-based validators, provides additional contract metadata

## Commonly-used Fields

- **NODE_ID**: Primary identifier for validator nodes and staking operations
- **VALIDATOR_TYPE**: Essential for understanding validator roles and network architecture
- **PROJECT_NAME**: Important for governance transparency and validator operator identification 

{% enddocs %}