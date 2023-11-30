{% test check_mismatch_percentage(model, threshold_percentage) %}

WITH latest_day AS (
    SELECT
        DATE_TRUNC('day', MAX(_inserted_timestamp)) AS last_ingestion_day
    FROM
        {{ model }}
),
api_call AS (
    SELECT
        *
    FROM
        {{ model }}
    WHERE
        _inserted_timestamp >= (SELECT last_ingestion_day FROM latest_day)
        AND _inserted_timestamp < (SELECT last_ingestion_day FROM latest_day) + INTERVAL '1 day'
        AND contract = 'A.e4cf4bdc1751c65d.AllDay'
),
FLATTEN_RES AS (
    SELECT
        ARRAY_SIZE(requested_ids) AS requested_ids_length,
        ARRAY_SIZE(res:data:data:searchMomentNFTsV2:edges) AS res_length
    FROM
        api_call
),
mismatch_calc AS (
    SELECT
        *,
        CASE
            WHEN requested_ids_length = 0 THEN 0
            ELSE ABS(requested_ids_length - res_length) / requested_ids_length * 100
        END AS mismatch_percentage
    FROM
        FLATTEN_RES
)
SELECT
    *
FROM
    mismatch_calc
WHERE
    mismatch_percentage > {{ threshold_percentage }}

{% endtest %}
