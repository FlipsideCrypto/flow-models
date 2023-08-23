SHELL := /bin/bash

# set default target
DBT_TARGET ?= sbx
AWS_LAMBDA_ROLE ?= aws_lambda_flow_api_sbx

dbt-console: 
	docker-compose run dbt_console

.PHONY: dbt-console

sl-flow-api:
	dbt run-operation create_aws_flow_api \
	--profile flow \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt/

udfs:
	dbt run-operation create_udfs \
	--vars '{"UPDATE_UDFS_AND_SPS":True}' \
	--profile flow \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt/

complete:
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/silver/streamline/core/complete \
	--profile flow \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt


grant-streamline-privileges:
	dbt run-operation grant_streamline_privileges \
	--profile flow \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt/ \
	--args '{role: $(AWS_LAMBDA_ROLE)}'

streamline: sl-flow-api udfs grant-streamline-privileges streamline_bronze

streamline_bronze:
	dbt run \
	--vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": False}' \
	-m 1+models/silver/streamline/bronze \
	--profiles-dir ~/.dbt \
	--target $(DBT_TARGET) \
	--profile flow