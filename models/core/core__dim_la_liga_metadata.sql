{{ config (
    materialized = 'view',
    enabled=false
) }}

SELECT
    nft_id,
    player_country,
    match_highlighted_team,
    match_season,
    player_position,
    play_type,
    match_date,
    player_last_name,
    play_data_id,
    match_day,
    play_time,
    player_number,
    player_first_name,
    player_known_name,
    match_home_team,
    match_away_team,
    match_home_score,
    match_away_score,
    player_jersey_name,
    play_half,
    player_data_id
FROM
    -- TODO
    -- probably remap this to select from the new dapper metadata silver table
    -- but this is all useless if moments cannot be linked to plays
    -- and all we have on chain rn are play IDs
    {{ ref('silver__nft_la_liga_play_metadata') }}
