{{ config(
    materialized = 'table',
    cluster_by = ['nft_id'],
    unique_key = 'nft_id'
) }}

WITH metadata AS (

    SELECT
        *
    FROM
        {{ source(
            'flow_external',
            'topshot_moments_minted_metadata_api'
        ) }}
        qualify ROW_NUMBER() over (
            PARTITION BY id
            ORDER BY
                DATA :getMintedMoment :data :acquiredAt :: TIMESTAMP
        ) = 1
),
FINAL AS (
    SELECT
        id AS nft_id,
        contract AS nft_collection,
        DATA :getMintedMoment :data :id :: STRING AS nbatopshot_id,
        DATA :getMintedMoment :data :flowSerialNumber :: NUMBER AS serial_number,
        DATA :getMintedMoment :data :setPlay :circulationCount :: NUMBER AS total_circulation,
        DATA :getMintedMoment :data :play :description :: VARCHAR AS moment_description,
        DATA :getMintedMoment :data :play :stats :playerName :: STRING AS player,
        DATA :getMintedMoment :data :play :stats :teamAtMoment :: STRING AS team,
        DATA :getMintedMoment :data :play :stats :nbaSeason :: STRING AS season,
        DATA :getMintedMoment :data :play :stats :playCategory :: STRING AS play_category,
        DATA :getMintedMoment :data :play :stats :playType :: STRING AS play_type,
        DATA :getMintedMoment :data :play :stats :dateOfMoment :: TIMESTAMP AS moment_date,
        DATA :getMintedMoment :data :set :flowName :: STRING AS set_name,
        DATA :getMintedMoment :data :set :flowSeriesNumber :: NUMBER AS set_series_number,
        DATA :getMintedMoment :data :play :assets :videos :: ARRAY AS video_urls,
        DATA :getMintedMoment :data :play :stats :: OBJECT AS play_stats_full,
        DATA :getMintedMoment :data :play :statsPlayerGameScores :: OBJECT AS player_game_stats,
        DATA :getMintedMoment :data :play :statsPlayerSeasonAverageScores :: OBJECT AS player_season_stats
    FROM
        metadata
    WHERE
        DATA :getMintedMoment :: STRING IS NOT NULL
)
SELECT
    *
FROM
    FINAL
