{{ config(
    materialized = 'view',
    post_hook = 'call silver.sp_bulk_get_topshot_moments_minted_metadata()'
) }}

SELECT
    event_contract,
    event_data :momentID :: STRING AS moment_id
FROM
    {{ ref('silver__events_final') }}
WHERE
    event_contract = 'A.0b2a3299cc857e29.TopShot'
    AND event_type = 'MomentMinted'
EXCEPT
SELECT
    contract,
    id AS moment_id
FROM
    {{ source(
        'flow_external',
        'topshot_moments_minted_metadata_api'
    ) }}
LIMIT
    3000
