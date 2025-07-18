{% docs evm_transfers_table_doc %}

This table contains all native asset transfers, including equivalent USD amounts. The origin addresses correspond to the to and from addresses from the `fact_transactions` table. The `identifier` and `tx_hash` columns relate this table back to `fact_traces`, which contains more details on the transfers. 

Below are the specific native tokens that correspond to each EVM chain:

| Status     | Description |
|------------|-------------|
| ETHEREUM   | ETH         |
| BINANCE    | BNB         |
| POLYGON    | POL         |
| AVALANCHE  | AVAX        |
| ARBITRUM   | ETH         |
| OPTIMISM   | ETH         |
| GNOSIS     | xDAI        |
| KAIA       | KLAY        |
| BASE       | ETH         |
| FLOW       | FLOW        |

{% enddocs %}


{% docs evm_transfer_table_doc %}

This table contains all events in the `fact_token_transfers` table, along with joined columns such as token price, symbol, and decimals where possible that allow for easier analysis of token transfer events. Please note native asset transfers are not included here.

{% enddocs %}


{% docs evm_fact_token_transfers_table_doc %}

This fact-based table contains emitted event logs for ERC-20 Token Transfers (e.g. `Transfer`: topic_0 = `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`). The contract address is the token transferred, and the raw amount field is the amount of tokens transferred. The values in this table are not decimal adjusted, instead please use `core.dim_contracts` or `core.ez_token_transfers` to reference decimals or decimal adjusted values. This table does not contain ERC-721 and ERC-1155 token transfers, instead please use `nft.ez_nft_transfers`. Additionally, this table does not contain transfers of the chain's native asset, instead please use `core.ez_native_transfers`.

{% enddocs %}


{% docs evm_ez_token_transfers_table_doc %}

## Description

A curated and enriched table containing token transfer events across EVM-compatible blockchains. This table consolidates token transfer data from event logs, providing a comprehensive view of all token movements including ERC-20, ERC-721, and other token standards. The table includes enriched metadata such as token names, symbols, decimals, and USD values, making it the primary source for token transfer analysis. Each record represents a single token transfer with complete context and enriched data.

## Key Use Cases

- **Token Transfer Analysis**: Tracking token movements and transfer patterns across EVM chains
- **Portfolio Tracking**: Monitoring token holdings and transfer activities for specific addresses
- **Token Economics**: Analyzing token distribution, circulation, and economic activity
- **DeFi Analysis**: Understanding token flows in DeFi protocols and yield farming activities
- **Cross-Chain Token Analysis**: Comparing token transfer patterns across different EVM-compatible blockchains
- **Token Performance**: Monitoring token usage, adoption, and transfer volume trends

## Important Relationships

- **core_evm__fact_event_logs**: Links to underlying event logs that generated these transfers
- **core_evm__fact_transactions**: Links to transactions that triggered these transfers
- **core_evm__dim_contracts**: Links to token contract metadata for transfer analysis
- **core_evm__dim_labels**: Links to address labels for user and contract identification
- **price__ez_prices_hourly**: Links to token prices for USD value calculations
- **core__ez_token_transfers**: May provide comparison data with native Flow token transfers

## Commonly-used Fields

- **FROM_ADDRESS/TO_ADDRESS**: Essential for transfer flow analysis and user behavior studies
- **CONTRACT_ADDRESS**: Critical for token identification and contract-specific analysis
- **AMOUNT/AMOUNT_USD**: Important for transfer value analysis and economic activity tracking
- **TOKEN_STANDARD**: Key for token type categorization and standard-specific analysis
- **SYMBOL/NAME**: Critical for token identification and user interface display
- **BLOCK_TIMESTAMP**: Essential for time-series analysis and temporal transfer tracking

{% enddocs %}


{% docs evm_ez_native_transfers_table_doc %}

## Description

A curated and enriched table containing native token transfer events across EVM-compatible blockchains. This table consolidates native token (e.g., ETH, BNB, AVAX) transfer data from execution traces, providing a comprehensive view of all native token movements. The table includes enriched metadata such as USD values and transfer context, making it the primary source for native token transfer analysis. Each record represents a single native token transfer with complete context and enriched data.

## Key Use Cases

- **Native Token Analysis**: Tracking native token movements and transfer patterns across EVM chains
- **Value Flow Analysis**: Understanding how native tokens flow through the ecosystem
- **Gas Economics**: Analyzing gas fee payments and native token usage for transaction costs
- **Cross-Chain Native Analysis**: Comparing native token transfer patterns across different EVM-compatible blockchains
- **User Behavior Analysis**: Understanding how users transfer native tokens for various purposes
- **Network Economics**: Monitoring native token distribution and economic activity

## Important Relationships

- **core_evm__fact_traces**: Links to underlying execution traces that generated these transfers
- **core_evm__fact_transactions**: Links to transactions that triggered these transfers
- **core_evm__dim_labels**: Links to address labels for user and contract identification
- **price__ez_prices_hourly**: Links to native token prices for USD value calculations
- **core_evm__ez_token_transfers**: May provide comparison data with token transfers
- **core__ez_token_transfers**: May provide comparison data with native Flow transfers

## Commonly-used Fields

- **FROM_ADDRESS/TO_ADDRESS**: Essential for transfer flow analysis and user behavior studies
- **AMOUNT/AMOUNT_USD**: Important for transfer value analysis and economic activity tracking
- **TYPE**: Critical for transfer type categorization and analysis
- **TRACE_INDEX**: Key for transfer ordering and execution sequence analysis
- **TX_HASH**: Essential for transaction linking and blockchain verification
- **BLOCK_TIMESTAMP**: Critical for time-series analysis and temporal transfer tracking

{% enddocs %}


{% docs evm_amount %}

Native asset value transferred, this is imported from the fsc-evm package.

{% enddocs %}


{% docs evm_amount_usd %}

Native asset value transferred, in USD.

{% enddocs %}


{% docs evm_log_id_transfers %}

This is the primary key for this table. This is a concatenation of the transaction hash and the event index at which the transfer event occurred. This field can be used to find more details on the event within the `fact_event_logs` table.

{% enddocs %}


{% docs evm_origin_from %}

The from address at the transaction level. 

{% enddocs %}


{% docs evm_origin_to %}

The to address at the transaction level. 

{% enddocs %}


{% docs evm_transfer_amount %}

The decimal transformed amount for this token. Tokens without a decimal adjustment will be nulled out here. 

{% enddocs %}


{% docs evm_transfer_amount_precise %}

The decimal transformed amount for this token returned as a string to preserve precision. Tokens without a decimal adjustment will be nulled out here.

{% enddocs %}


{% docs evm_transfer_amount_usd %}

The amount in US dollars for this transfer at the time of the transfer. Tokens without a decimal adjustment or price will be nulled out here. 

{% enddocs %}


{% docs evm_transfer_contract_address %}

Contract address of the token being transferred.

{% enddocs %}


{% docs evm_transfer_from_address %}

The sending address of this transfer.

{% enddocs %}


{% docs evm_transfer_has_decimal %}

Whether or not our contracts model contains the necessary decimal adjustment for this token. 

{% enddocs %}


{% docs evm_transfer_has_price %}

Whether or not our prices model contains this hourly token price. 

{% enddocs %}


{% docs evm_transfer_raw_amount %}

The amount of tokens transferred. This value is not decimal adjusted. 

{% enddocs %}


{% docs evm_transfer_raw_amount_precise %}

The amount of tokens transferred returned as a string to preserve precision. This value is not decimal adjusted.

{% enddocs %}


{% docs evm_transfer_to_address %}

The receiving address of this transfer. This can be a contract address. 

{% enddocs %}


{% docs evm_transfer_token_price %}

The price, if available, for this token at the transfer time. 

{% enddocs %}


{% docs evm_transfer_tx_hash %}

Transaction hash is a unique 66-character identifier that is generated when a transaction is executed. This will not be unique in this table as a transaction could include multiple transfer events.

{% enddocs %}


{% docs evm_token_standard %}

The type of token transferred in this event, for example `erc721`, `erc1155`, or `erc20`.

{% enddocs %}