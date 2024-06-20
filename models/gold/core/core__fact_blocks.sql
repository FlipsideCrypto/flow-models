{{ config(
    materialized = 'view',
    tags = ['scheduled']
) }}

SELECT
    blocks_id,
    block_height,
    block_timestamp,
    'mainnet' AS network,
    network_version,
    'flow' AS chain_id,
    tx_count,
    id,
    parent_id,
    COALESCE (
        blocks_id,
        {{ dbt_utils.generate_surrogate_key(['block_height']) }}
    ) AS fact_blocks_id,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__streamline_blocks') }}
