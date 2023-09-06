{{ config(
    materialized = 'view',
    tags = ['livequery', 'topshot', 'moment_metadata']
) }}

WITH mints AS (

    SELECT
        event_contract,
        event_data :momentID :: STRING AS moment_id
    FROM
        {{ ref('silver__nft_moments') }}
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
),
lq_always_null AS (
    SELECT
        moment_id,
        event_contract,
        COUNT(1) AS num_times_null_resp
    FROM
        {{ target.database }}.livequery.null_moments_metadata
    WHERE
        event_contract = 'A.0b2a3299cc857e29.TopShot'
    GROUP BY
        1,
        2
    HAVING
        num_times_null_resp > 2
),
legacy_always_null AS (
    SELECT
        id,
        contract,
        COUNT(1) AS num_times_null_resp
    FROM
        {{ ref('streamline__null_moments_metadata') }}
    WHERE
        contract = 'A.0b2a3299cc857e29.TopShot'
    GROUP BY
        1,
        2
    HAVING
        num_times_null_resp > 2
)
SELECT
    DISTINCT *
FROM
    all_topshots
WHERE
    moment_id NOT IN (
        (
            SELECT
                nft_id AS moment_id
            FROM
                {{ target.database }}.silver.nft_topshot_metadata
            UNION
            SELECT
                id AS moment_id
            FROM
                legacy_always_null
            UNION
            SELECT
                moment_id
            FROM
                lq_always_null
        )
    )
