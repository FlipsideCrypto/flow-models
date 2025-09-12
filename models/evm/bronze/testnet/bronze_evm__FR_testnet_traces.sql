{{ config (
    materialized = 'view',
    tags = ['traces']
) }}

{{ streamline_external_table_FR_query_v2(
    model = "evm_testnet_traces",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER )"
) }}
