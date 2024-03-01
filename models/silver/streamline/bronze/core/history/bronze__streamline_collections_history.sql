{{ config (
    materialized = 'view'
) }}

{#
{% set 
    table_names = 
    [
        'COLLECTIONS_CANDIDATE_07', 'COLLECTIONS_CANDIDATE_08', 'COLLECTIONS_CANDIDATE_09', 'COLLECTIONS_MAINNET_01', 'COLLECTIONS_MAINNET_02', 'COLLECTIONS_MAINNET_03', 'COLLECTIONS_MAINNET_04', 'COLLECTIONS_MAINNET_05', 'COLLECTIONS_MAINNET_06', 'COLLECTIONS_MAINNET_07', 'COLLECTIONS_MAINNET_08', 'COLLECTIONS_MAINNET_09', 'COLLECTIONS_MAINNET_10', 'COLLECTIONS_MAINNET_11', 'COLLECTIONS_MAINNET_12', 'COLLECTIONS_MAINNET_13', 'COLLECTIONS_MAINNET_14', 'COLLECTIONS_MAINNET_15', 'COLLECTIONS_MAINNET_16', 'COLLECTIONS_MAINNET_17', 'COLLECTIONS_MAINNET_18', 'COLLECTIONS_MAINNET_19', 'COLLECTIONS_MAINNET_20', 'COLLECTIONS_MAINNET_21', 'COLLECTIONS_MAINNET_22'
    ]
%}
#}

{% set 
    table_names = 
    [
        'COLLECTIONS_MAINNET_18'
    ]
%}

{{ streamline_multiple_external_table_query(
    table_names,
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_name = "_partition_by_block_id",
    unique_key = "id"
) }}
