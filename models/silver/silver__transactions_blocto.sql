{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
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
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
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
