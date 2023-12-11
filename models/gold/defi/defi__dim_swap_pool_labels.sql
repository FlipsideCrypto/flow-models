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
        NULL AS labels_pools_metapier_id,
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
        {{ ref('silver__labels_pools_metapier') }}
),
pairs_s AS (
    SELECT
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
    COALESCE (
        streamline_transaction_id,
        {{ dbt_utils.generate_surrogate_key(['tx_id']) }}
    ) AS labels_pools_id,
    swap_contract,
    deployment_timestamp,
    token0_contract,
    token1_contract,
    pool_id,
    vault_address,
    COALESCE (
        inserted_timestamp,
        _inserted_timestamp
    ) AS inserted_timestamp,
    COALESCE (
        modified_timestamp,
        _inserted_timestamp
    ) AS modified_timestamp
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY labels_pools_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
