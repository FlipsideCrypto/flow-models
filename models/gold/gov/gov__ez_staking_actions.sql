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
        chainwalkers
    UNION ALL
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
        streamline
)
SELECT
    *
FROM
    FINAL
