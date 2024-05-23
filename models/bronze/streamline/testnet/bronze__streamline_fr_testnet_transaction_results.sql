{{ config (
    materialized = 'view'
) }}

{% set model = this.identifier.split("_")[-2:] | join('_') %}
{{ streamline_external_table_FR_query(
    model,
    source_id = "bronze_streamline_testnet",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_name = "_partition_by_block_id",
    unique_key = "id"
) }}


