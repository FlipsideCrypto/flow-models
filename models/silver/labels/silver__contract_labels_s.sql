{{ config(
    materialized = 'incremental',
    cluster_by = ['event_contract'],
    unique_key = 'event_contract',
    tags = ['scheduled', 'streamline_scheduled']
) }}

WITH splt AS (

    SELECT
        event_contract,
        SPLIT(
            event_contract,
            '.'
        ) AS ec_s
    FROM
        {{ ref('silver__streamline_events') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    DISTINCT event_contract,
    ec_s [array_size(ec_s)-1] :: STRING AS contract_name,
    CONCAT(
        '0x',
        ec_s [array_size(ec_s)-2] :: STRING
    ) AS account_address
FROM
    splt
WHERE
    ec_s [0] != 'flow'
