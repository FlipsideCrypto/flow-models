{{ config(
    materialized = 'view',
    tags = ['scheduled']
) }}

WITH chainwalkers AS (

    SELECT
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_index,
        event_contract,
        event_type,
        event_data
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        block_height < {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
),
streamline AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_index,
        event_contract,
        event_type,
        event_data
    FROM
        {{ ref('silver__streamline_events') }}
    WHERE
        block_height >= {{ var(
            'STREAMLINE_START_BLOCK'
        ) }}
)
SELECT
    *
FROM
    chainwalkers
UNION ALL
SELECT
    *
FROM
    streamline
