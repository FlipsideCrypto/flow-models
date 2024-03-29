{{ config(
    materialized = 'view',
    tags = ['livequery', 'allday', 'moment_metadata'],
    enabled = False
) }}
{# AllDay workflow inactive, view migrated to lq naming convention only #}
WITH mints AS (

    SELECT
        event_contract,
        event_data :id :: STRING AS moment_id
    FROM
        {{ ref('silver__nft_moments') }}
    WHERE
        event_contract = 'A.e4cf4bdc1751c65d.AllDay'
        AND event_type = 'MomentNFTMinted'
),
sales AS (
    SELECT
        nft_collection AS event_contract,
        nft_id AS moment_id
    FROM
        {{ ref('silver__nft_sales') }}
    WHERE
        nft_collection = 'A.e4cf4bdc1751c65d.AllDay'
),
all_day_ids AS (
    SELECT
        *
    FROM
        mints
    UNION
    SELECT
        *
    FROM
        sales
)
SELECT
    *
FROM
    all_day_ids
EXCEPT
SELECT
    nft_collection AS event_contract,
    nft_id AS moment_id
FROM
    {{ ref('silver__nft_allday_metadata') }}
