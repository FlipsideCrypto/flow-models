{{ config(
    materialized = 'view',
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'NFT, ALLDAY'
            }
        }
    }
) }}

WITH allday AS (

    SELECT
        nft_id,
        nft_collection,
        nflallday_id,
        serial_number,
        moment_tier,
        total_circulation,
        moment_description,
        player,
        team,
        season,
        week,
        classification,
        play_type,
        moment_date,
        series,
        set_name,
        video_urls,
        moment_stats_full
    FROM
        {{ ref('silver__nft_allday_metadata') }}
    EXCEPT
    SELECT
        nft_id,
        nft_collection,
        nflallday_id,
        serial_number,
        moment_tier,
        total_circulation,
        moment_description,
        player,
        team,
        season,
        week,
        classification,
        play_type,
        moment_date,
        series,
        set_name,
        video_urls,
        moment_stats_full
    FROM
        {{ ref('silver__allday_moments_metadata_error') }}
)
SELECT
    *
FROM
    allday
