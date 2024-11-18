{{ config (
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    unique_key = 'dim_address_mapping_id',
    cluster_by = ['block_timestamp_associated::date'],
    post_hook = 'ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(evm_address, flow_address);',
    tags = ['scheduled_non_core']
) }}

SELECT
    block_timestamp AS block_timestamp_associated,
    block_height AS block_height_associated,
    flow_address,
    evm_address,
    flow_evm_address_map_id AS dim_address_mapping_id,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp
FROM
    {{ ref('silver__flow_evm_address_map') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
