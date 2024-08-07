{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    unique_key = 'nft_allday_metadata_s_id',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(nft_id,nflallday_id);",
    tags = ['nft']
) }}

WITH api_call AS (

    SELECT
        *
    FROM
        {{ source(
            'bronze_api',
            'allday_metadata'
        ) }}
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
),
FLATTEN_RES AS (
    SELECT
        flattened_array.value:node as data,
        api_call.res:status_code as status_code,
        SYSDATE() as _inserted_timestamp,
        contract
    FROM api_call,
    LATERAL FLATTEN(input => api_call.res:data:data:searchMomentNFTsV2:edges) as flattened_array
    WHERE api_call.res:status_code = 200 
    AND data IS NOT NULL
),

FINAL AS (
    SELECT
        DATA :flowID  :: STRING AS nft_id,
        {{ dbt_utils.generate_surrogate_key(
            ['nft_id']
        ) }} AS nft_allday_metadata_s_id,
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
        _inserted_timestamp,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        FLATTEN_RES
)
SELECT
    *
FROM
    FINAL
qualify ROW_NUMBER() over (
        PARTITION BY nft_allday_metadata_s_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1