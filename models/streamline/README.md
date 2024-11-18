# Setup Snowflake Api Integration & UDFS

## Setup Snowflake Api Integration

Use the [create_aws_flow_api()](../../macros/streamline/api_integrations.sql#2) macro to create the `streamline-flow` Snowflake API integration.

The 

```zsh
DBT_TARGET=sbx make sl-flow-api

# This runs:
# 	dbt run-operation create_aws_flow_api \
# 	--profile flow \
# 	--target $(DBT_TARGET) \
# 	--profiles-dir ~/.dbt/
```

```zsh
# call sbx udf_bulk_grpc() to test the API integration
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' -m 1+models/silver/streamline/core/history/streamline__get_blocks_history.sql --profile flow --target sbx --profiles-dir ~/.dbt
```


```zsh 
# dev bronze__streamline_blocks.sql
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' -m 1+models/silver/streamline/bronze/core/realtime/bronze__streamline_blocks.sql --profile flow --target dev --profiles-dir ~/.dbt
```

```zsh
# dev complete_get_blocks
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' -m 1+models/silver/streamline/core/complete/streamline__complete_get_blocks.sql --profile flow --target dev --profiles-dir ~/.dbt

# dev complete_get_collections
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' -m 1+models/silver/streamline/core/complete/streamline__complete_get_collections.sql --profile flow --target dev --profiles-dir ~/.dbt

# dev complete_get_transactions
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' -m 1+models/silver/streamline/core/complete/streamline__complete_get_transactions.sql --profile flow --target dev --profiles-dir ~/.dbt

# dev get_blocks_history
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' -m 1+models/silver/streamline/core/history/blocks/streamline__get_blocks_history_mainnet22.sql --profile flow --target dev --profiles-dir ~/.dbt

# dev get_collections_history
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' -m 1+models/silver/streamline/core/history/collections/streamline__get_collections_history_mainnet22.sql --profile flow --target dev --profiles-dir ~/.dbt

# dev get_transactions_history
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' -m 1+models/silver/streamline/core/history/transactions/streamline__get_transactions_history_mainnet22.sql --profile flow --target dev --profiles-dir ~/.dbt

# dev get_transaction_results_history
dbt run --vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' -m 1+models/silver/streamline/core/history/transaction_results/streamline__get_transaction_results_history_mainnet22.sql --profile flow --target dev --profiles-dir ~/.dbt

# dev bronze__streamline_blocks.sql
dbt run --select bronze__streamline_blocks.sql --profiles-dir ~/.dbt --target dev --profile flow --vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}'
```


```zsh
DBT_TARGET=dev \
AWS_LAMBDA_ROLE=AWS_LAMBDA_FLOW_API_DEV \
make  undo_clone_purge

DBT_TARGET=dev make streamline_bronze
```