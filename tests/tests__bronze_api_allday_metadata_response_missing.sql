{{ config(
    severity = "warn",
    error_if = ">100",
) }}

WITH latest_day AS (

    SELECT
        DATE_TRUNC('day', MAX(_inserted_timestamp)) AS last_ingestion_day
    FROM
        {{ source(
            'bronze_api',
            'allday_metadata'
        ) }}
),
api_call AS (
    SELECT
        *
    FROM
        {{ source(
            'bronze_api',
            'allday_metadata'
        ) }}
    WHERE
        _inserted_timestamp :: DATE = (
            SELECT
                last_ingestion_day
            FROM
                latest_day
        )
        AND contract = 'A.e4cf4bdc1751c65d.AllDay'
),
flatten_res AS (
    SELECT
        requested_ids,
        ARRAY_SIZE(requested_ids) AS requested_ids_length,
        ARRAY_SIZE(
            res :data :data :searchMomentNFTsV2 :edges
        ) AS res_length,
        _INSERTED_TIMESTAMP
    FROM
        api_call
),
mismatch_calc AS (
    SELECT
        requested_ids,
        CASE
            WHEN requested_ids_length = 0 THEN 0
            ELSE ABS(
                requested_ids_length - res_length
            ) / requested_ids_length * 100
        END AS mismatch_percentage,
        _INSERTED_TIMESTAMP
    FROM
        flatten_res
)
SELECT
    *
FROM
    mismatch_calc
WHERE
    mismatch_percentage > 30
