{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'nft_id',
    tags = ['streamline', 'topshot']
) }}
-- depends_on: {{ ref('bronze__streamline_topshot_metadata') }}
WITH metadata_from_streamline AS (

    SELECT
        VALUE :CONTRACT AS contract,
        VALUE :ID AS moment_id,
        DATA,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_topshot_metadata') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_topshot_metadata_FR') }}
{% endif %}
)
SELECT
    moment_id :: STRING AS nft_id,
    contract :: STRING AS nft_collection,
    DATA :data :getMintedMoment :data :id :: STRING AS nbatopshot_id,
    DATA :data :getMintedMoment :data :flowSerialNumber :: NUMBER AS serial_number,
    DATA :data :getMintedMoment :data :setPlay :circulationCount :: NUMBER AS total_circulation,
    DATA :data :getMintedMoment :data :play :description :: VARCHAR AS moment_description,
    DATA :data :getMintedMoment :data :play :stats :playerName :: STRING AS player,
    DATA :data :getMintedMoment :data :play :stats :teamAtMoment :: STRING AS team,
    DATA :data :getMintedMoment :data :play :stats :nbaSeason :: STRING AS season,
    DATA :data :getMintedMoment :data :play :stats :playCategory :: STRING AS play_category,
    DATA :data :getMintedMoment :data :play :stats :playType :: STRING AS play_type,
    DATA :data :getMintedMoment :data :play :stats :dateOfMoment :: TIMESTAMP AS moment_date,
    DATA :data :getMintedMoment :data :set :flowName :: STRING AS set_name,
    DATA :data :getMintedMoment :data :set :flowSeriesNumber :: NUMBER AS set_series_number,
    DATA :data :getMintedMoment :data :play :assets :videos :: ARRAY AS video_urls,
    DATA :data :getMintedMoment :data :play :stats :: OBJECT AS moment_stats_full,
    DATA :data :getMintedMoment :data :play :statsPlayerGameScores :: OBJECT AS player_stats_game,
    DATA :data :getMintedMoment :data :play :statsPlayerSeasonAverageScores :: OBJECT AS player_stats_season_to_date,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['nft_id']
    ) }} AS nft_topshot_metadata_v2_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    metadata_from_streamline
WHERE
    DATA :errors IS NULL qualify ROW_NUMBER() over (
        PARTITION BY nft_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
