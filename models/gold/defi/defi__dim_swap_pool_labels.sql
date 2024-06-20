{{ config (
    materialized = 'view',
    tag = ['scheduled']
) }}

WITH pairs_s AS (

    SELECT
        tx_id,
        labels_pools_id AS labels_pools_metapier_id,
        swap_contract,
        deployment_timestamp,
        token0_contract,
        token1_contract,
        pool_id,
        vault_address,
        inserted_timestamp,
        _inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__labels_pools_s') }}
),
metapier_s AS (
    SELECT
        tx_id,
        labels_pools_metapier_id,
        swap_contract,
        deployment_timestamp,
        token0_contract,
        token1_contract,
        pool_id,
        vault_address,
        inserted_timestamp,
        _inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__labels_pools_metapier_s') }}
),
FINAL AS (
    SELECT
        *
    FROM
        pairs_s
    UNION ALL
    SELECT
        *
    FROM
        metapier_s
)
SELECT
    swap_contract,
    deployment_timestamp,
    token0_contract,
    token1_contract,
    pool_id,
    vault_address,
    COALESCE (
        labels_pools_metapier_id,
        {{ dbt_utils.generate_surrogate_key(['tx_id']) }}
    ) AS dim_swap_pool_labels_id,
    COALESCE (
        inserted_timestamp,
        _inserted_timestamp
    ) AS inserted_timestamp,
    COALESCE (
        modified_timestamp,
        _inserted_timestamp
    ) AS modified_timestamp
FROM
    FINAL
