{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['event_contract'],
    unique_key = 'event_contract',
    tags = ['scheduled', 'chainwalkers_scheduled']
) }}

WITH splt AS (

    SELECT
        event_contract,
        SPLIT(
            event_contract,
            '.'
        ) AS ec_s,
        _inserted_timestamp
    FROM
        {{ ref('silver__events') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
FINAL AS (
    SELECT
        DISTINCT event_contract,
        ec_s [array_size(ec_s)-1] :: STRING AS contract_name,
        CONCAT(
            '0x',
            ec_s [array_size(ec_s)-2] :: STRING
        ) AS account_address,
        _inserted_timestamp
    FROM
        splt
    WHERE
        ec_s [0] != 'flow'
)
SELECT
    *
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY event_contract
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
