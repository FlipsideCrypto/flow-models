-- depends_on: {{ ref('silver__nft_allday_metadata_s') }}
{{ config(
    materialized = 'view',
    tags = ['livequery', 'allday', 'moment_metadata'],
) }}

{% set table_exists = check_table_exists('SILVER', 'NFT_ALLDAY_METADATA_S') %}

WITH mints AS (

    SELECT
        event_contract,
        event_data :id :: STRING AS moment_id
    FROM
        {{ ref('silver__nft_moments_s') }}
    WHERE
        event_contract = 'A.e4cf4bdc1751c65d.AllDay'
        AND event_type = 'MomentNFTMinted'
),
sales AS (
    SELECT
        nft_collection AS event_contract,
        nft_id AS moment_id
    FROM
        {{ ref('silver__nft_sales_s') }}
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
{% if table_exists | trim == '"True"' %}
EXCEPT
    SELECT
        nft_collection AS event_contract,
        nft_id AS moment_id
    FROM
        {{ ref('silver__nft_allday_metadata_s') }}
{% endif %}