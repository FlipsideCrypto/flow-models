{{ config(
    materialized = 'view',
    tags = ['testnet']
) }}

SELECT
    tx_id,
    block_timestamp,
    block_height :: INT AS block_height,
    tx_succeeded,
    event_index,
    event_contract,
    event_type,
    event_data,
    COALESCE (
        streamline_event_id,
        {{ dbt_utils.generate_surrogate_key(['tx_id']) }}
    ) AS fact_events_id,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__testnet_events') }}
