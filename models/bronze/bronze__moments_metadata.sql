{{ config (
    materialized = 'view',
    tags = ['scheduled']
) }}

SELECT
    id,
    contract,
    DATA,
    VALUE,
    _inserted_date,
    TO_TIMESTAMP_NTZ(SUBSTR(SPLIT_PART(metadata$filename, '/', 4), 1, 10) :: NUMBER, 0) AS _inserted_timestamp
FROM
    {{ source(
        'bronze_streamline',
        'moments_minted_metadata_api'
    ) }}
