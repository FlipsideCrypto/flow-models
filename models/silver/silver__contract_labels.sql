{{ config(
    materialized = 'table',
    cluster_by = ['event_contract'],
    unique_key = 'event_contract'
) }}

WITH splt AS (

    SELECT
        event_contract,
        SPLIT(
            event_contract,
            '.'
        ) AS ec_s
    FROM
        {{ ref('silver__events') }}
)
SELECT
    DISTINCT *
FROM
    (
        SELECT
            event_contract,
            ec_s [array_size(ec_s)-1] :: STRING AS contract_name,
            CONCAT(
                '0x',
                ec_s [array_size(ec_s)-2] :: STRING
            ) AS account_address
        FROM
            splt
        WHERE
            ec_s [0] != 'flow'
    )
