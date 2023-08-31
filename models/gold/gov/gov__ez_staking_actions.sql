{{ config(
    materialized = 'view',
    tags = ['ez', 'scheduled'],
    meta={
    'database_tags':{
        'table': {
            'PURPOSE': 'STAKING'
            }
        }
    }
) }}

WITH staking_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__staking_actions') }}
),
FINAL AS (
    SELECT
        tx_id,
        event_index,
        block_timestamp,
        block_height,
        tx_succeeded,
        delegator,
        action,
        amount,
        node_id
    FROM
        staking_actions
)
SELECT
    *
FROM
    FINAL
