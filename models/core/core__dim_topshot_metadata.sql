{{ config (
    materialized = 'view'
) }}

WITH topshot AS (

    SELECT
        nft_id,
        nft_collection,
        nbatopshot_id,
        serial_number,
        total_circulation,
        moment_description,
        player,
        team,
        season,
        play_category,
        play_type,
        moment_date,
        set_name,
        set_series_number,
        video_urls,
        moment_stats_full,
        player_stats_game,
        player_stats_season_to_date,
    FROM
        {{ ref('silver__nft_topshot_metadata') }}
)
SELECT
    *
FROM
    topshot
