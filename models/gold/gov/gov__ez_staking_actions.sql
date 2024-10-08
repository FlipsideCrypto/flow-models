{{ config(
    materialized = 'view',
    tags = ['ez', 'scheduled'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STAKING' }} }
) }}

SELECT
    tx_id,
    event_index,
    block_timestamp,
    block_height,
    tx_succeeded,
    delegator,
    delegator_id,
    action,
    amount,
    node_id,
    COALESCE (
        staking_actions_id,
        {{ dbt_utils.generate_surrogate_key(['tx_id']) }}
    ) AS ez_staking_actions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__staking_actions') }}
