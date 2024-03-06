SHELL := /bin/bash

# set default target
DBT_TARGET ?= dev
AWS_LAMBDA_ROLE ?= aws_lambda_flow_api_dev
INVOKE_STREAMS ?= True

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


udf_2:
	dbt run-operation create_udf_bulk_grpc_us_east_2 \
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
	--vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/silver/streamline/bronze \
	--profiles-dir ~/.dbt \
	--target $(DBT_TARGET) \
	--profile flow

blocks_history:
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/silver/streamline/core/history/blocks/streamline__get_blocks_history_mainnet22.sql \
	--profile flow \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt

collections_history:
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/silver/streamline/core/history/collections/streamline__get_collections_history_mainnet22.sql \
	--profile flow \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt

tx_history:
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/silver/streamline/core/history/transactions/streamline__get_transactions_history_mainnet22.sql \
	--profile flow \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt

tx_results_history:
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS":True, "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/silver/streamline/core/history/transaction_results/streamline__get_transaction_results_history_mainnet_18.sql \
	--profile flow \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt

tx_results_batch_history:
	dbt run \
	--vars '{"STREAMLINE_INVOKE_STREAMS": $(INVOKE_STREAMS), "STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	-m 1+models/silver/streamline/core/history/transaction_results/batch/streamline__get_batch_transaction_results_history_mainnet_18.sql \
	--profile flow \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt

lq_overloads:
	dbt run \
	-s models/deploy/core/ \
	--profile flow \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt \
	--vars '{"UPDATE_EPHEMERAL_UDFS":True}'

bronze:
	dbt run \
	-s bronze__streamline_transaction_results_history \
	--vars '{"STREAMLINE_USE_DEV_FOR_EXTERNAL_TABLES": True}' \
	--profiles-dir ~/.dbt \
	--target $(DBT_TARGET)
