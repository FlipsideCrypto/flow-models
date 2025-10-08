{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
) }}

WITH legacy AS (
    SELECT
        batch_id AS point_id,
        created_at,
        batch_index,
        transfer_index,
        from_address,
        to_address,
        boxes,
        keys,
        points,
        NULL AS direction,
        NULL AS amount_start,
        NULL AS amount_end,
        NULL AS account_id,
        NULL AS user_id,
        from_address AS user_wallet_address,
        NULL AS transaction_id,
        NULL AS data,
        NULL AS partition_key,
        NULL AS index,
        NULL AS _inserted_timestamp,
        points_transfers_id AS fact_points_transfers_id,
        request_date,
        inserted_timestamp,
        modified_timestamp,
        'legacy' AS source
    FROM
        {{ ref('silver_api__points_transfers') }}
),

new_data AS (
    SELECT
        entry_id AS point_id,
        created_at,
        INDEX AS batch_index,
        NULL AS transfer_index,
        user_wallet_address AS from_address,
        NULL AS to_address,
        NULL AS boxes,
        NULL AS keys,
        amount AS points,
        direction,
        amount_start,
        amount_end,
        account_id,
        user_id,
        user_wallet_address,
        transaction_id,
        data,
        partition_key,
        INDEX AS index,
        _inserted_timestamp,
        reward_points_spend_id AS fact_points_transfers_id,
        DATE_TRUNC('day', created_at) AS request_date,
        inserted_timestamp,
        modified_timestamp,
        'snag' AS source
    FROM
        {{ ref('silver_api__reward_points_spend') }}
),
FINAL AS (SELECT
    point_id,
    source,
    created_at,
    batch_index,
    transfer_index,
    from_address,
    to_address,
    boxes,
    keys,
    points,
    direction,
    amount_start,
    amount_end,
    account_id,
    user_id,
    user_wallet_address,
    transaction_id,
    data,
    partition_key,
    index,
    _inserted_timestamp
FROM legacy
UNION ALL
SELECT
    point_id,
    source,
    created_at,
    batch_index,
    transfer_index,
    from_address,
    to_address,
    boxes,
    keys,
    points,
    direction,
    amount_start,
    amount_end,
    account_id,
    user_id,
    user_wallet_address,
    transaction_id,
    data,
    partition_key,
    index,
    _inserted_timestamp
FROM new_data
)

SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['point_id', 'source', 'created_at', 'batch_index', 'transfer_index']) }} AS fact_points_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM FINAL