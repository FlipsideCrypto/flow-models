{% docs evm_blocks_table_doc %}

## Description

A comprehensive fact table containing block-level data for EVM-compatible blockchains. This table serves as the foundation for blockchain analysis, providing detailed information about each block including metadata, gas usage, mining information, and blockchain state. The table supports analysis of blockchain performance, gas economics, and network health across multiple EVM chains. Each record represents a single block with complete header information and aggregated transaction data.

## Key Use Cases

- **Blockchain Performance Analysis**: Monitoring block production rates, gas usage patterns, and network efficiency
- **Gas Economics**: Analyzing gas prices, fee structures, and economic incentives across different EVM chains
- **Network Health Monitoring**: Tracking block difficulty, mining rewards, and network security metrics
- **Cross-Chain Comparison**: Comparing performance and characteristics across different EVM-compatible blockchains
- **Mining Analysis**: Understanding validator/miner behavior and reward distribution patterns
- **Blockchain State Tracking**: Monitoring blockchain progression and state changes over time

## Important Relationships

- **core_evm__fact_transactions**: Links to transaction data within each block
- **core_evm__fact_event_logs**: May link to event logs emitted in blocks
- **core_evm__fact_traces**: May link to execution traces for block analysis
- **core__fact_blocks**: May provide comparison data with native Flow blocks
- **price__ez_prices_hourly**: May link to native token prices for economic analysis

## Commonly-used Fields

- **BLOCK_NUMBER**: Essential for block identification and chronological analysis
- **BLOCK_TIMESTAMP**: Critical for time-series analysis and temporal data aggregation
- **GAS_USED/GAS_LIMIT**: Key for gas economics and network efficiency analysis
- **TX_COUNT**: Important for transaction volume and network activity analysis
- **MINER**: Critical for mining analysis and validator behavior studies
- **BASE_FEE_PER_GAS**: Essential for EIP-1559 fee mechanism analysis

{% enddocs %}


{% docs evm_block_header_json %}

This JSON column contains the block header details.

{% enddocs %}


{% docs evm_blockchain %}

The blockchain on which transactions are being confirmed.

{% enddocs %}


{% docs evm_blocks_hash %}

The hash of the block header for a given block.

{% enddocs %}


{% docs evm_blocks_nonce %}

Block nonce is a value used during mining to demonstrate proof of work for a given block.

{% enddocs %}


{% docs evm_difficulty %}

The effort required to mine the block.

{% enddocs %}


{% docs evm_extra_data %}

Any data included by the miner for a given block.

{% enddocs %}


{% docs evm_gas_limit %}

Total gas limit provided by all transactions in the block.

{% enddocs %}


{% docs evm_gas_used %}

Total gas used in the block.

{% enddocs %}

{% docs evm_network %}

The network on the blockchain used by a transaction.

{% enddocs %}


{% docs evm_parent_hash %}

The hash of the block from which a given block is generated. Also known as the parent block.

{% enddocs %}


{% docs evm_receipts_root %}

The root of the state trie.

{% enddocs %}


{% docs evm_sha3_uncles %}

The mechanism which Ethereum Javascript RLP encodes an empty string.

{% enddocs %}


{% docs evm_size %}

Block size, which is determined by a given block's gas limit.

{% enddocs %}


{% docs evm_total_difficulty %}

Total difficulty of the chain at a given block.

{% enddocs %}


{% docs evm_tx_count %}

Total number of transactions within a block.

{% enddocs %}


{% docs evm_uncle_blocks %}

Uncle blocks occur when two blocks are mined and broadcasted at the same time, with the same block number. The block validated across the most nodes will be added to the primary chain, and the other one becomes an uncle block. Miners do receive rewards for uncle blocks.

{% enddocs %}

{% docs evm_miner %}

The address of the beneficiary to whom the mining rewards were given

{% enddocs %}

{% docs evm_state_root %}

The root of the final state trie of the block

{% enddocs %}

{% docs evm_transactions_root %}

The root of the transaction trie of the block

{% enddocs %}

{% docs evm_logs_bloom %}

The bloom filter for the logs of the block.

{% enddocs %}

{% docs evm_mix_hash %}

A string of a 256-bit hash encoded as a hexadecimal

{% enddocs %}

{% docs evm_base_fee_per_gas %}

A string of the base fee encoded in hexadecimal format. Please note that this response field will not be included in a block requested before the EIP-1559 upgrade

{% enddocs %}

{% docs evm_blob_gas_used %}

The amount of gas used for blob transactions in the block.

{% enddocs %}

{% docs evm_excess_blob_gas %}

The amount of excess blob gas in the block.

{% enddocs %}

{% docs evm_parent_beacon_block_root %}

The root of the parent beacon block.

{% enddocs %}

{% docs evm_withdrawals %}

The withdrawals for the block.

{% enddocs %}

{% docs evm_withdrawals_root %}

The root of the withdrawals for the block.

{% enddocs %}