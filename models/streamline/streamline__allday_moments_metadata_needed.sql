{{ config(
    materialized = 'view',
) }}

WITH mints AS (

    SELECT
        event_contract,
        event_data :id :: NUMBER AS nft_id
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        event_contract = 'A.e4cf4bdc1751c65d.AllDay'
        AND event_type = 'MomentNFTMinted'
),
sales AS (
    SELECT
        nft_collection AS event_contract,
        nft_id
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
LIMIT
    3000
