{% docs core_decoded_event_logs_table_doc %}

For information on how to submit a contract for decoding, as well as how ABIs are sourced, please visit [here](https://science.flipsidecrypto.xyz/abi-requestor/).
      This model contains decoded event logs for contracts that we have an ABI for. Please note, this table does not include all event logs, only those that we have an ABI for.
      The `decoded_log` column is the easiest place to query decoded data. It is a JSON object, where the keys are the names of the event parameters, and the values are the values of the event parameters.
      You can select from this column using the following sample format, `decoded_log:from::string` or more generally, `decoded_log:<event_param>::datatype`. See below for a full sample query.
      The `full_decoded_logs` column contains the same information, as well as additional fields such as the datatype of the decoded data. You may need to laterally flatten this column to query the data.
            
      Sample query for USDC Transfer events:
      
      ```sql
      select 
      tx_hash,
      block_number,
      contract_address,
      decoded_log:from::string as from_address,
      decoded_log:to::string as to_address,
      decoded_log:value::integer as value
      from ethereum.core.fact_decoded_event_logs
      where contract_address = lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')
      and block_number between 16400000 and 16405000
      and event_name = 'Transfer'
      limit 50```

{% enddocs %}

{% docs evm_contract_name %}

The name of the contract, if the contract has a name() function.

{% enddocs %}


{% docs evm_event_name %}

The name of the event, as defined in the contract ABI.

{% enddocs %}


{% docs evm_decoded_log %}

The flattened decoded log, where the keys are the names of the event parameters, and the values are the values of the event parameters.

{% enddocs %}


{% docs evm_full_decoded_log %}

The full decoded log, including the event name, the event parameters, and the data type of the event parameters.

{% enddocs %}

{% docs evm_contract_address %}

The address of the contract that the event log was emitted from.

{% enddocs %}