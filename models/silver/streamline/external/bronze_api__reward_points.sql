{{ config (
    materialized = 'view',
    tags = ['streamline_non_core']
) }}

{{ streamline_external_table_query_v2(
    model = "reward_points",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER )"
) }}
