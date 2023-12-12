{{ config(
    materialized = 'view',
    tags = ['scheduled']
) }}

WITH chainwalkers AS (

    SELECT
        NULL AS blocks_id,
        block_height,
        block_timestamp,
        network,
        network_version,
        chain_id,
        tx_count,
        id,
        parent_id,
        _inserted_timestamp,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_height < {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
streamline AS (
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
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__streamline_blocks') }}
    WHERE
        block_height >= {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
FINAL AS (
    SELECT
        *
    FROM
        chainwalkers
    UNION ALL
    SELECT
        *
    FROM
        streamline
)
SELECT
    blocks_id,
    block_height,
    block_timestamp,
    network,
    network_version,
    chain_id,
    tx_count,
    id,
    parent_id,
    COALESCE (
        blocks_id,
        {{ dbt_utils.generate_surrogate_key(['block_height']) }}
    ) AS fact_blocks_id,
    COALESCE (
        inserted_timestamp,
        _inserted_timestamp
    ) AS inserted_timestamp,
    COALESCE (
        modified_timestamp,
        _inserted_timestamp
    ) AS modified_timestamp
FROM
    FINAL
