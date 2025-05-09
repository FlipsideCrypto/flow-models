{{ config(
    materialized = 'view'
) }}

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
    _INSERTED_TIMESTAMP,
    nft_moment_metadata_topshot_id,
    inserted_timestamp,
    modified_timestamp,
    _INVOCATION_ID
FROM
    {{ source(
        'silver',
        'nft_topshot_metadata'
    ) }}
