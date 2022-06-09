{{ config(
    materialized = 'incremental',
    cluster_by = ['_ingested_at::DATE', 'block_timestamp::DATE'],
    unique_key = 'tx_id',
    incremental_strategy = 'delete+insert'
) }}

WITH silver_txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}

{% if is_incremental() %}
WHERE
    _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
),
blocto_txs AS (
    SELECT
        *
    FROM
        silver_txs
    WHERE
        LOWER(payer) = LOWER('0x55AD22F01EF568A1') -- Blocto network fee paying address
)
SELECT
    *
FROM
    blocto_txs
