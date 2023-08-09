{{ config(
    materialized = 'view',
    tags = ['livequery', 'topshot', 'moment_metadata']
) }}

WITH mints AS (

    SELECT
        block_timestamp,
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
        block_timestamp,
        nft_collection AS event_contract,
        nft_id AS moment_id
    FROM
        {{ ref('silver__nft_sales') }}
    WHERE
        nft_collection ILIKE '%topshot%'
),
all_topshots AS (
    SELECT
        block_timestamp,
        event_contract,
        moment_id
    FROM
        mints
    UNION
    SELECT
        block_timestamp,
        event_contract,
        moment_id
    FROM
        sales
),
always_null AS (
    SELECT
        id,
        contract,
        COUNT(*) AS num_times_null_resp
    FROM
        {# TODO - update once migrated to livequery schema #}
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
                {{ ref('silver__nft_topshot_metadata') }}
            UNION
            SELECT
                id AS moment_id
            FROM
                always_null
        )
    )
