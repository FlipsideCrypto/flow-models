{{ config(
    materialized = 'view'
) }}

WITH silver_blocks AS (

    SELECT
        *
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_timestamp >= '2022-04-20'
),
gold_blocks AS (
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
        silver_blocks
)
SELECT
    *
FROM
    gold_blocks
