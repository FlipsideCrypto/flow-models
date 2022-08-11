{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id'
) }}

WITH silver_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
delegate AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_contract,
        event_data :amount :: FLOAT AS amount,
        event_data :delegatorID :: STRING AS delegator_id,
        event_data :nodeID :: STRING AS node_id,
        'delegate' AS action,
        _inserted_timestamp
    FROM
        silver_events
    WHERE
        event_type = 'DelegatorTokensCommitted'
)
