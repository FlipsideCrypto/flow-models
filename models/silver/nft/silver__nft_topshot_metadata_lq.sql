{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'nft_id',
    tags = ['scheduled']
) }}

WITH metadata AS (

    SELECT
        _res_id AS id,
        'A.0b2a3299cc857e29.TopShot' AS contract,
        -- Note MUST lowercase the object keys. Autoformat will capitalize
        DATA :data :data :: variant AS DATA,
        _inserted_timestamp
    FROM
        {{ ref('livequery__request_topshot_metadata') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY id
    ORDER BY
        DATA :data :data :getMintedMoment :data :acquiredAt :: TIMESTAMP
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
        DATA :getMintedMoment :data :play :stats :: OBJECT AS moment_stats_full,
        DATA :getMintedMoment :data :play :statsPlayerGameScores :: OBJECT AS player_stats_game,
        DATA :getMintedMoment :data :play :statsPlayerSeasonAverageScores :: OBJECT AS player_stats_season_to_date,
        _inserted_timestamp
    FROM
        metadata
    WHERE
        DATA :getMintedMoment :: STRING IS NOT NULL
)
SELECT
    *
FROM
    FINAL
