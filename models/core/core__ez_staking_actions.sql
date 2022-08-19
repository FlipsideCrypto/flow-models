{{ config(
    materialized = 'view'
) }}

WITH staking_actions AS (

    SELECT
        *
    FROM
        {{ ref('silver__staking_actions') }}
    WHERE
        block_timestamp :: DATE >= '2022-04-20'
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
