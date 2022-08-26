{{ config (
    materialized = 'view'
) }}

SELECT
    id,
    contract,
    DATA,
    VALUE,
    TO_TIMESTAMP_NTZ(SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10) :: NUMBER, 0) AS _inserted_timestamp
FROM
    {{ source(
        'flow_external',
        'moments_metadata_api'
    ) }}
WHERE
    contract = 'A.0b2a3299cc857e29.TopShot'