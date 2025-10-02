{{ config (
    materialized = 'view',
    tags = ['streamline_realtime', 'account_storage']
) }}

{{ streamline_external_table_query_v2(
    model = "account_storage",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER )"
) }}
