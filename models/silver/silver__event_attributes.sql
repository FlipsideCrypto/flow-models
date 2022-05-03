{{ config(
  materialized = 'incremental',
  cluster_by = ['ingested_at::DATE', 'block_timestamp::DATE'],
  unique_key = #TODO,
  incremental_strategy = 'delete+insert'
) }}

