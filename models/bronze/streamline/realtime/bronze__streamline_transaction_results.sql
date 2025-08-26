{{ config (
    materialized = 'view'
) }}

{% set model = this.identifier.split("_")[-2:] | join('_') %}
{{ streamline_external_table_query(
    model = 'transaction_results_v2',
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_name = "_partition_by_block_id",
    unique_key = "id"
) }}
