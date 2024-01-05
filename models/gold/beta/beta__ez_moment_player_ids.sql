{{ config(
    materialized = 'view'
) }}

WITH espn AS (

    SELECT
        CASE
            -- slight name mismatches
            WHEN A.full_name = 'Patrick Mahomes' THEN 'Patrick Mahomes II'
            WHEN A.full_name = 'Joshua Palmer' THEN 'Josh Palmer'
            ELSE A.full_name
        END AS player,
        t.display_name AS team,
        A.id AS espn_player_id,
        SPLIT(
            SPLIT(TRY_PARSE_JSON(A.team) :"$ref", 'http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/seasons/2023/teams/') [1],
            '?'
        ) [0] :: INT AS espn_team_id,
        try_parse_json(status):type::string = 'active' as is_active -- note, this may depend on time of data pull from ESPN. Includes IR status. Update as needed.
    FROM
        {{ ref('beta__dim_nfl_athletes') }} A
        LEFT JOIN {{ ref('beta__dim_nfl_teams') }}
        t
        ON espn_team_id = t.id
    WHERE
        SPLIT(
            SPLIT(TRY_PARSE_JSON(POSITION) :parent :"$ref", 'http://sports.core.api.espn.com/v2/sports/football/leagues/nfl/positions/') [1],
            '?') [0] :: INT = 70 -- offense only
        ),
        allday AS (
            SELECT
                nft_id,
                nflallday_id,
                serial_number,
                moment_tier,
                CASE
                    -- Some moments use DJ others D.J.
                    WHEN player = 'D.J. Moore' THEN 'DJ Moore'
                    ELSE player
                END AS player,
                team,
                season,
                play_type,
                moment_stats_full :metadata :playerPosition :: STRING AS POSITION
            FROM
                {{ ref('nft__dim_allday_metadata') }}
                ad
            WHERE
                classification = 'PLAYER_GAME'
                AND season >= 2018
                AND POSITION IN (
                    'QB',
                    'WR',
                    'RB',
                    'TE'
                )
        ),
        FINAL AS (
            SELECT
                nft_id,
                nflallday_id,
                serial_number,
                moment_tier,
                ad.player,
                ad.team,
                season,
                play_type,
                POSITION,
                espn_player_id,
                espn_team_id,
                is_active
            FROM
                allday ad
                LEFT JOIN espn
                ON LOWER(
                    ad.player
                ) = LOWER(
                    espn.player
                )
        )
    SELECT
        *
    FROM
        FINAL
