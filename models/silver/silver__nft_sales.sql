{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_ingested_at::DATE, block_timestamp::DATE'],
    unique_key = 'tx_id'
) }}

WITH topshot AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_topshot_sales') }}

{% if is_incremental() %}
WHERE
    _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
)
SELECT
    *
FROM
    topshot
