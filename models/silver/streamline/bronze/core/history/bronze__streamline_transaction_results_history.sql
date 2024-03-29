{{ config (
    materialized = 'view'
) }}

{# 
    Full array to table names, keeping commented for posterity.
    {% set 
    table_names = 
    [
        'TRANSACTION_RESULTS_CANDIDATE_07', 'TRANSACTION_RESULTS_CANDIDATE_08', 'TRANSACTION_RESULTS_CANDIDATE_09', 'TRANSACTION_RESULTS_MAINNET_01', 'TRANSACTION_RESULTS_MAINNET_02', 'TRANSACTION_RESULTS_MAINNET_03', 'TRANSACTION_RESULTS_MAINNET_04', 'TRANSACTION_RESULTS_MAINNET_05', 'TRANSACTION_RESULTS_MAINNET_06', 'TRANSACTION_RESULTS_MAINNET_07', 'TRANSACTION_RESULTS_MAINNET_08', 'TRANSACTION_RESULTS_MAINNET_09', 'TRANSACTION_RESULTS_MAINNET_10', 'TRANSACTION_RESULTS_MAINNET_11', 'TRANSACTION_RESULTS_MAINNET_12', 'TRANSACTION_RESULTS_MAINNET_13', 'TRANSACTION_RESULTS_MAINNET_14', 'TRANSACTION_RESULTS_MAINNET_15', 'TRANSACTION_RESULTS_MAINNET_16', 'TRANSACTION_RESULTS_MAINNET_17', 'TRANSACTION_RESULTS_MAINNET_18', 'TRANSACTION_RESULTS_MAINNET_19', 'TRANSACTION_RESULTS_MAINNET_20', 'TRANSACTION_RESULTS_MAINNET_21', 'TRANSACTION_RESULTS_MAINNET_22'
    ]
%} #}
{# 
    TODO - below array includes NVs with 99.99% coverage. For example Mainnet 5 is missing 532 txs.
    These are edge cases to be investigated after the final sporks get to near-complete status.
{% set 
    table_names = 
    [
        'TRANSACTION_RESULTS_MAINNET_05', 'TRANSACTION_RESULTS_MAINNET_06', 'TRANSACTION_RESULTS_MAINNET_09', 'TRANSACTION_RESULTS_MAINNET_10', 'TRANSACTION_RESULTS_MAINNET_11', 'TRANSACTION_RESULTS_MAINNET_12', 'TRANSACTION_RESULTS_MAINNET_13', 'TRANSACTION_RESULTS_MAINNET_14', 'TRANSACTION_RESULTS_MAINNET_15', 'TRANSACTION_RESULTS_MAINNET_16', 'TRANSACTION_RESULTS_MAINNET_17', 'TRANSACTION_RESULTS_MAINNET_18', 'TRANSACTION_RESULTS_MAINNET_19', 'TRANSACTION_RESULTS_MAINNET_20', 'TRANSACTION_RESULTS_MAINNET_22'
    ]
%} #}

{% set 
    table_names = 
    [
        'TRANSACTION_RESULTS_MAINNET_14', 'TRANSACTION_RESULTS_MAINNET_15', 'TRANSACTION_RESULTS_MAINNET_16', 'TRANSACTION_RESULTS_MAINNET_17', 'TRANSACTION_RESULTS_MAINNET_18', 'TRANSACTION_RESULTS_MAINNET_19', 'TRANSACTION_RESULTS_MAINNET_22'
    ]
%}

{{ streamline_multiple_external_table_query(
    table_names,
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER)",
    partition_name = "_partition_by_block_id",
    unique_key = "id"
) }}
