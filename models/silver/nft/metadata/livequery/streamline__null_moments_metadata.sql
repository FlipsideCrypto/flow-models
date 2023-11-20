{{ config(
    materialized = 'incremental',
    unique_key = ["id","contract","_inserted_date"],
    tags = ['topshot', 'moment_metadata'],
    enabled = True
) }}
{# Legacy workflow - TODO deprecate soon #}

SELECT
    id,
    contract,
    _inserted_date,
    _inserted_timestamp
FROM
    {{ ref('bronze__moments_metadata') }}
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
