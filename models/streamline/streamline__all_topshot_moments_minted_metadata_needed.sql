{{ config(
    materialized = 'view',
) }}

WITH mints AS (

    SELECT
        event_contract,
        event_data :momentID :: STRING AS moment_id
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        event_contract = 'A.0b2a3299cc857e29.TopShot'
        AND event_type = 'MomentMinted'
),
sales AS (
    SELECT
        nft_collection AS event_contract,
        nft_id AS moment_id
    FROM
        {{ ref('silver__nft_sales') }}
    WHERE
        nft_collection ILIKE '%topshot%'
),
all_topshots AS (
    SELECT
        event_contract,
        moment_id
    FROM
        mints
    UNION
    SELECT
        event_contract,
        moment_id
    FROM
        sales
)
SELECT
    DISTINCT *
FROM
    all_topshots
EXCEPT
SELECT
    contract,
    id AS moment_id
FROM
    {{ source(
        'flow_external',
        'moments_metadata_api'
    ) }}
WHERE
    contract = 'A.0b2a3299cc857e29.TopShot'
