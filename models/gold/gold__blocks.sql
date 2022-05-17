{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'block_height'
) }}

WITH silver_blocks AS (

    SELECT
        *
    FROM
        {{ ref('silver__blocks') }}

{% if is_incremental() %}
WHERE
    _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
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
