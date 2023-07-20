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