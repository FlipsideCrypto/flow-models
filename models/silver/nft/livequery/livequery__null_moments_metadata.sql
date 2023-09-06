{{ config(
    materialized = 'incremental',
    unique_key = '_id',
    tags = ['livequery', 'topshot', 'moment_metadata']
) }}

SELECT
    moment_id,
    event_contract,
    _inserted_date,
    _inserted_timestamp,
    MD5(
        'moment_id' || 'event_contract' || '_inserted_date'
    ) AS _id
FROM
    {{ target.database }}.livequery.request_topshot_metadata
WHERE
    DATA :data :data :getMintedMoment :: STRING IS NULL

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
