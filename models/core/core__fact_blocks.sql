{{ config(
    materialized = 'view'
) }}

WITH silver_blocks AS (

    SELECT
        *
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_timestamp >= '2022-05-09'
),
gold_blocks AS (
    SELECT
        block_height,
        block_timestamp,
        network,
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
