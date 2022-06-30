{{ config(
    materialized = 'view',
) }}

SELECT
    DISTINCT event_contract,
    event_data :id :: STRING AS id
FROM
    flow_dev.silver.events_final
WHERE
    event_contract IN (
        SELECT
            REPLACE(
                VALUE :id :: STRING,
                'FLOW:',
                ''
            )
        FROM
            {{ source(
                'flow_external',
                'rarible_collections_api'
            ) }}
        WHERE
            VALUE :id :: STRING <> 'FLOW:A.0b2a3299cc857e29.TopShot'
    )
EXCEPT
SELECT
    contract,
    id
FROM
    {{ source(
        'flow_external',
        'rarible_nft_metadata_api'
    ) }}
LIMIT
    50000
