{{ config(
    materialized = 'view',
    tags = ['ez', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }}}
) }}

WITH chainwalkers AS (

    SELECT
        *
    FROM
        {{ ref('silver__staking_actions') }}
    WHERE
        block_height < {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
streamline AS (
    SELECT
        *
    FROM
        {{ ref('silver__staking_actions_s') }}
    WHERE
        block_height >= {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
FINAL AS (
    SELECT
        NULL AS stacking_actions_id,
        tx_id,
        event_index,
        block_timestamp,
        block_height,
        tx_succeeded,
        delegator,
        action,
        amount,
        node_id,
        _inserted_timestamp,
        NULL AS inserted_timestamp,
        NULL AS modified_timestamp
    FROM
        chainwalkers
    UNION ALL
    SELECT
        stacking_actions_id,
        tx_id,
        event_index,
        block_timestamp,
        block_height,
        tx_succeeded,
        delegator,
        action,
        amount,
        node_id,
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp
    FROM
        streamline
)
SELECT
    COALESCE (
        stacking_actions_id,
        {{ dbt_utils.generate_surrogate_key(['tx_id']) }}
    ) AS stacking_actions_id,
    tx_id,
    event_index,
    block_timestamp,
    block_height,
    tx_succeeded,
    delegator,
    action,
    amount,
    node_id,
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
        PARTITION BY stacking_actions_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
