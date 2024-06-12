# Backfill

Bronze backfill models have been parametrized to load one network version at a time, as each set of NVs and method responses is a separate bucket and external table.  
  
Run either an individual model type (blocks, collections, transactions, transaction_results) or all 4 at once with `tag:streamline_load`.

```shell
dbt run -s 1+tag:streamline_load --vars '{"LOAD_BACKFILL": True, "LOAD_BACKFILL_VERSION": "<NV>"}'
```

## Valid Network Versions
  - CANDIDATE_07
  - CANDIDATE_08
  - CANDIDATE_09
  - MAINNET_01
  - MAINNET_02
  - MAINNET_03
  - MAINNET_04
  - MAINNET_05
  - MAINNET_06
  - MAINNET_07
  - MAINNET_08
  - MAINNET_09
  - MAINNET_10
  - MAINNET_11
  - MAINNET_12
  - MAINNET_13
  - MAINNET_14
  - MAINNET_15
  - MAINNET_16
  - MAINNET_17
  - MAINNET_18
  - MAINNET_19
  - MAINNET_20
  - MAINNET_21
  - MAINNET_22

## View Types
Views with the word `complete` in the name are used in the complete history models at `models/silver/streamline/core/complete`. These use a macro to scan multiple external tables in one call, and feed the streamline backfill process.  

The views `bronze__streamline_<method>_history` query just one network version based on the `LOAD_BACKFILL_VERSION` argument passed at runtime. No default is set for this variable so execution fails if it is forgottten.  

## Running Streamline Backfill
If a a network version requires more backfill due to missing blocks or transactions (at present, there are 5800 missing transaction results), run the following command as the workflow dbt_run_history has been deleted.  
```shell
dbt run -s 2+streamline__get_<method>_history_<network_version> --vars '{"STREAMLINE_INVOKE_STREAMS": True}'
```

i.e.
```shell
dbt run -s \
2+streamline__get_transaction_results_history_mainnet_22 \
--vars '{"STREAMLINE_INVOKE_STREAMS": True}'
```
