{{ config(
    materialized = 'incremental',
    cluster_by = ['nft_id'],
    unique_key = 'nft_id',
    incremental_strategy = 'delete+insert'
) }}

WITH liga_plays AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_la_liga_events') }}
    WHERE
        event_type = 'PlayCreated'
),
play_metadata AS (
    SELECT
        event_data :id :: NUMBER AS nft_id_raw,
        VALUE :key :value :: STRING AS column_header,
        VALUE :value :value :: STRING AS column_value
    FROM
        liga_plays,
        LATERAL FLATTEN(input => TRY_PARSE_JSON(event_data :metadata))
    WHERE
        event_type = 'PlayCreated'
),
FINAL AS (
    SELECT
        *
    FROM
        play_metadata pivot(MAX(column_value) for column_header IN ('PlayerCountry', 'MatchHighlightedTeam', 'MatchSeason', 'PlayerPosition', 'PlayType', 'MatchDate', 'PlayerLastName', 'PlayDataID', 'MatchDay', 'PlayTime', 'PlayerNumber', 'PlayerFirstName', 'PlayerKnownName', 'MatchHomeTeam', 'MatchAwayTeam', 'MatchHomeScore', 'MatchAwayScore', 'PlayerJerseyName', 'PlayHalf', 'PlayerDataID')) AS p (
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
        )
)
SELECT
    *
FROM
    FINAL
