{{ config (
    materialized = 'view',
    tag = ['scheduled']
) }}

WITH pairs AS (

    SELECT
        swap_contract,
        deployment_timestamp,
        token0_contract,
        token1_contract,
        pool_id,
        vault_address
    FROM
        {{ ref('silver__labels_pools') }}
),
metapier AS (
    SELECT
        swap_contract,
        deployment_timestamp,
        token0_contract,
        token1_contract,
        pool_id,
        vault_address
    FROM
        {{ ref('silver__labels_pools_metapier') }}
),
FINAL AS (
    SELECT
        *
    FROM
        pairs
    UNION
    SELECT
        *
    FROM
        metapier
)
SELECT
    *
FROM
    FINAL