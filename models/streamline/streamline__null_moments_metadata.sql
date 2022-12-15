{{ config(
    materialized = 'incremental',
    unique_key = ["id","contract","_inserted_date"]
) }}

SELECT
    id,
    contract,
    _inserted_date,
    TO_TIMESTAMP_NTZ(SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10) :: NUMBER, 0) AS _inserted_timestamp
FROM
    {{ source(
        'bronze_streamline',
        'moments_minted_metadata_api'
    ) }}
WHERE
    DATA :getMintedMoment :: STRING IS NULL

{% if is_incremental() %}
AND _inserted_date >= (
    SELECT
        MAX(_inserted_date)
    FROM
        {{ this }}
)
AND _inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
AND _inserted_date >= '2022-12-09'
{% endif %}
