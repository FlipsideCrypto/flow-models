{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'nft_id'
) }}

WITH metadata AS (

    SELECT
        *
    FROM
        {{ ref('bronze__moments_metadata') }}
    WHERE
        contract = 'A.e4cf4bdc1751c65d.AllDay'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY id
    ORDER BY
        _inserted_timestamp
) = 1
),
FINAL AS (
    SELECT
        id AS nft_id,
        contract AS nft_collection,
        DATA :id :: STRING AS nflallday_id,
        DATA :serialNumber :: NUMBER AS serial_number,
        DATA: edition :tier :: STRING AS moment_tier,
        DATA :edition :currentMintSize :: NUMBER AS total_circulation,
        DATA :edition :play :metadata :description :: VARCHAR AS moment_description,
        IFF(
            DATA :edition :play :metadata :playerFullName :: STRING = '',
            'N/A',
            DATA :edition :play :metadata :playerFullName :: STRING
        ) AS player,
        DATA :edition :play :metadata: teamName :: STRING AS team,
        DATA :edition :play :metadata :season :: STRING AS season,
        DATA :edition :play :metadata: week :: STRING AS week,
        DATA :edition :play :metadata: classification :: STRING AS classification,
        DATA :edition :play :metadata :playType :: STRING AS play_type,
        DATA :edition :play :metadata :gameDate :: TIMESTAMP AS moment_date,
        DATA :edition :series :name :: STRING AS series,
        DATA :edition: set :name :: STRING AS set_name,
        DATA :edition :play :metadata :videos :: ARRAY AS video_urls,
        DATA :edition :play :: OBJECT AS moment_stats_full,
        _inserted_timestamp
    FROM
        metadata
)
SELECT
    *
FROM
    FINAL
