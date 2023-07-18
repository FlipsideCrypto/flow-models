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