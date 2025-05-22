{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'nft_id',
    tags = ['livequery', 'topshot'],
    full_refresh = False,
    enabled = false
) }}
{# NFT Metadata from legacy process lives in external table, deleted CTE and set FR=False
TO

LIMIT
    / avoid unnecessary TABLE scans #}
    WITH metadata_lq AS (
        SELECT
            _res_id,
            'A.0b2a3299cc857e29.TopShot' AS contract,
            moment_id,
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
),
lq_final AS (
    SELECT
        moment_id :: STRING AS nft_id,
        contract :: STRING AS nft_collection,
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
        metadata_lq
    WHERE
        DATA :getMintedMoment :: STRING IS NOT NULL
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['nft_id']
    ) }} AS nft_moment_metadata_topshot_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    lq_final qualify ROW_NUMBER() over (
        PARTITION BY nft_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
