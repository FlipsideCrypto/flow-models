{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'nft_id',
    tags = ['streamline', 'topshot'],
    full_refresh = False,
    enabled = False
) }}
-- The key change is to source data directly from the bronze_streamline external table
-- rather than from the LiveQuery model
WITH metadata_from_streamline AS (

    SELECT
        contract,
        id AS moment_id,
        DATA,
        _INSERTED_DATE AS _inserted_timestamp
    FROM
        {{ source(
            'bronze_streamline',
            'moments_minted_metadata_api'
        ) }}

{% if is_incremental() %}
WHERE
    _INSERTED_DATE >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
-- Process only successful API responses
successful_responses AS (
    SELECT
        moment_id AS nft_id,
        contract AS nft_collection,
        DATA :data :data :getMintedMoment :data :id :: STRING AS nbatopshot_id,
        DATA :data :data :getMintedMoment :data :flowSerialNumber :: NUMBER AS serial_number,
        DATA :data :data :getMintedMoment :data :setPlay :circulationCount :: NUMBER AS total_circulation,
        DATA :data :data :getMintedMoment :data :play :description :: VARCHAR AS moment_description,
        DATA :data :data :getMintedMoment :data :play :stats :playerName :: STRING AS player,
        DATA :data :data :getMintedMoment :data :play :stats :teamAtMoment :: STRING AS team,
        DATA :data :data :getMintedMoment :data :play :stats :nbaSeason :: STRING AS season,
        DATA :data :data :getMintedMoment :data :play :stats :playCategory :: STRING AS play_category,
        DATA :data :data :getMintedMoment :data :play :stats :playType :: STRING AS play_type,
        DATA :data :data :getMintedMoment :data :play :stats :dateOfMoment :: TIMESTAMP AS moment_date,
        DATA :data :data :getMintedMoment :data :set :flowName :: STRING AS set_name,
        DATA :data :data :getMintedMoment :data :set :flowSeriesNumber :: NUMBER AS set_series_number,
        DATA :data :data :getMintedMoment :data :play :assets :videos :: ARRAY AS video_urls,
        DATA :data :data :getMintedMoment :data :play :stats :: OBJECT AS moment_stats_full,
        DATA :data :data :getMintedMoment :data :play :statsPlayerGameScores :: OBJECT AS player_stats_game,
        DATA :data :data :getMintedMoment :data :play :statsPlayerSeasonAverageScores :: OBJECT AS player_stats_season_to_date,
        _inserted_timestamp
    FROM
        metadata_from_streamline
    WHERE
        DATA :data :data :getMintedMoment :data IS NOT NULL
) -- Final selection with surrogate key
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['nft_id']
    ) }} AS nft_moment_metadata_topshot_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    successful_responses qualify ROW_NUMBER() over (
        PARTITION BY nft_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
