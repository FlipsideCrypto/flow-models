SHELL := /bin/bash

# set default target
DBT_TARGET ?= sbx

dbt-console: 
	docker-compose run dbt_console

.PHONY: dbt-console

sl-flow-api:
	dbt run-operation create_aws_flow_api \
	--profile flow \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt/

udfs:
	dbt run-operation create_udf_get_chainhead \
	--profile flow \
	--target $(DBT_TARGET) \
	--profiles-dir ~/.dbt/