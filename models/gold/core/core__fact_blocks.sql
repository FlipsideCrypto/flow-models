{{ config(
    materialized = 'view',
    tags = ['scheduled']
) }}

WITH chainwalkers AS (

    SELECT
        block_height,
        block_timestamp,
        network,
        network_version,
        chain_id,
        tx_count,
        id,
        parent_id
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_height < {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
streamline AS (
    SELECT
        block_height,
        block_timestamp,
        'mainnet' AS network,
        network_version,
        'flow' AS chain_id,
        tx_count,
        id,
        parent_id
    FROM
        {{ ref('silver__streamline_blocks') }}
    WHERE
        block_height >= {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
)
SELECT
    *
FROM
    chainwalkers
UNION ALL
SELECT
    *
FROM
    streamline
