{{ config (
    materialized = 'view',
    tags = ['streamline_evm_non_core', 'contract_abis']
) }}

{{ streamline_external_table_query_v2(
    model = "contract_abis",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER )"
) }}
