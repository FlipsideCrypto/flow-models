{{ config (
    materialized = 'view'
) }}

{% set 
table_names = [
    'blocks', 'BLOCKS_CANDIDATE_07', 'BLOCKS_CANDIDATE_08', 'BLOCKS_CANDIDATE_09', 'BLOCKS_MAINNET_01', 'BLOCKS_MAINNET_02', 'BLOCKS_MAINNET_03', 'BLOCKS_MAINNET_04', 'BLOCKS_MAINNET_05', 'BLOCKS_MAINNET_06', 'BLOCKS_MAINNET_07', 'BLOCKS_MAINNET_08', 'BLOCKS_MAINNET_09', 'BLOCKS_MAINNET_10', 'BLOCKS_MAINNET_11', 'BLOCKS_MAINNET_12', 'BLOCKS_MAINNET_13', 'BLOCKS_MAINNET_14', 'BLOCKS_MAINNET_15', 'BLOCKS_MAINNET_16', 'BLOCKS_MAINNET_17', 'BLOCKS_MAINNET_18', 'BLOCKS_MAINNET_19', 'BLOCKS_MAINNET_20', 'BLOCKS_MAINNET_21', 'BLOCKS_MAINNET_22'
]
%}

{{ streamline_multiple_external_table_query(
    table_names,
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_name = "_partition_by_block_id",
    unique_key = "id"
) }}
