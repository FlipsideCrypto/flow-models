{{ config (
    materialized = 'view',
    tags = ['streamline_non_core']
) }}
{{ streamline_external_table_query_v2(
    model = "topshot_metadata",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER )"
) }}
