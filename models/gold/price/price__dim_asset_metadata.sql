{{ config(
    materialized = 'view',
    tag = ['scheduled']
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    token_address,
    asset_id,
    A.symbol,
    A.name,
    platform AS blockchain,
    platform_id AS blockchain_id,
    provider,
    A.inserted_timestamp,
    A.modified_timestamp,
    A.complete_provider_asset_metadata_id AS dim_asset_metadata_id
FROM
    {{ ref('silver__complete_provider_asset_metadata') }} A