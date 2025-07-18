{% docs evm_traces_table_doc %}

## Description

A comprehensive fact table containing execution trace data for transactions on EVM-compatible blockchains. This table provides detailed information about the internal execution of transactions, including contract calls, value transfers, and execution paths. The table supports deep transaction analysis, debugging, and understanding complex contract interactions across multiple EVM chains. Each record represents a single execution step with complete trace information and execution context.

## Key Use Cases

- **Transaction Debugging**: Understanding why transactions failed or behaved unexpectedly
- **Contract Interaction Analysis**: Tracking complex multi-contract interactions and call chains
- **Value Flow Tracking**: Following native token and value transfers through complex transactions
- **Gas Usage Analysis**: Understanding detailed gas consumption patterns in contract execution
- **Security Analysis**: Identifying potential security issues and attack patterns
- **Cross-Chain Trace Analysis**: Comparing execution patterns across different EVM-compatible blockchains

## Important Relationships

- **core_evm__fact_transactions**: Links to transactions that generated these traces
- **core_evm__fact_blocks**: Links to blocks containing trace data
- **core_evm__dim_contracts**: Links to contract metadata for trace analysis
- **core_evm__ez_native_transfers**: May link to curated native transfer events
- **core_evm__fact_event_logs**: May link to events emitted during trace execution
- **core__fact_transactions**: May provide comparison data with native Flow transactions

## Commonly-used Fields

- **TRACE_INDEX**: Essential for trace ordering and execution sequence analysis
- **FROM_ADDRESS/TO_ADDRESS**: Critical for call flow analysis and contract interaction tracking
- **TYPE**: Important for trace categorization and execution type analysis
- **VALUE**: Key for value transfer tracking and economic analysis
- **GAS_USED**: Critical for gas consumption analysis and optimization studies
- **TRACE_SUCCEEDED**: Essential for execution success analysis and error tracking

{% enddocs %}


{% docs evm_traces_block_no %}

The block number of this transaction.

{% enddocs %}


{% docs evm_traces_blocktime %}

The block timestamp of this transaction.

{% enddocs %}


{% docs evm_traces_call_data %}

The raw JSON data for this trace.

{% enddocs %}


{% docs evm_traces_value %}

The amount of the native asset transferred in this trace.

{% enddocs %}


{% docs evm_traces_from %}

The sending address of this trace. This is not necessarily the from address of the transaction. 

{% enddocs %}


{% docs evm_traces_gas %}

The gas supplied for this trace.

{% enddocs %}


{% docs evm_traces_gas_used %}

The gas used for this trace.

{% enddocs %}


{% docs evm_traces_identifier %}

This field represents the position and type of the trace within the transaction. 

{% enddocs %}


{% docs evm_trace_index %}

The index of the trace within the transaction.

{% enddocs %}


{% docs evm_traces_input %}

The input data for this trace.

{% enddocs %}


{% docs evm_traces_output %}

The output data for this trace.

{% enddocs %}


{% docs evm_sub_traces %}

The amount of nested sub traces for this trace.

{% enddocs %}


{% docs evm_traces_to %}

The receiving address of this trace. This is not necessarily the to address of the transaction. 

{% enddocs %}


{% docs evm_traces_tx_hash %}

The transaction hash for the trace. Please note, this is not necessarily unique in this table as transactions frequently have multiple traces. 

{% enddocs %}


{% docs evm_traces_type %}

The type of internal transaction. Common trace types are `CALL`, `DELEGATECALL`, and `STATICCALL`.

{% enddocs %}

{% docs evm_trace_succeeded %}

The boolean value representing if the trace succeeded.

{% enddocs %}

{% docs evm_trace_error_reason %}

The reason for the trace failure, if any.

{% enddocs %}

{% docs evm_trace_address %}

The trace address for this trace.

{% enddocs %}


{% docs evm_revert_reason %}

The reason for the revert, if available.

{% enddocs %}

