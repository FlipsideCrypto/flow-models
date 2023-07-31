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

grant-streamline-privileges:
	dbt run-operation grant_streamline_privileges \
	--profile flow \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt/ \
	--args '{role: $(AWS_LAMBDA_ROLE)}'

undo_clone_purge: sl-flow-api udfs grant-streamline-privileges