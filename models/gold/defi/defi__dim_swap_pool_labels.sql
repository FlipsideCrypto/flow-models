{{ config (
    materialized = 'view',
    tag = ['scheduled']
) }}

WITH pairs_cw AS (

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
metapier_cw AS (
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
pairs_s AS (
    SELECT
        swap_contract,
        deployment_timestamp,
        token0_contract,
        token1_contract,
        pool_id,
        vault_address
    FROM
        {{ ref('silver__labels_pools_s') }}
),
metapier_s AS (
    SELECT
        swap_contract,
        deployment_timestamp,
        token0_contract,
        token1_contract,
        pool_id,
        vault_address
    FROM
        {{ ref('silver__labels_pools_metapier_s') }}
),
FINAL AS (
    SELECT
        *
    FROM
        pairs_cw
    UNION
    SELECT
        *
    FROM
        metapier_cw
    UNION
    SELECT
        *
    FROM
        pairs_s
    UNION
    SELECT
        *
    FROM
        metapier_s
)
SELECT
    *
FROM
    FINAL
