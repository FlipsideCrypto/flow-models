{{ config (
    materialized = 'view',
    tags = ['streamline_non_core', 'minting_assets']
) }}

{{ streamline_external_table_FR_query_v2(
    model = "minting_assets",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER )"
) }}
