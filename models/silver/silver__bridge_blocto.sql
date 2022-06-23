{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::date'],
    unique_key = "CONCAT_WS('-', tx_id, event_index)"
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}

{% if is_incremental() %}
WHERE
    _ingested_at :: DATE >= CURRENT_DATE -2
{% endif %}
),
teleports AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        event_contract AS event_contract_teleport,
        event_data :amount :: DOUBLE AS amount_teleport,
        event_data :from AS from_teleport,
        STRTOK_TO_ARRAY(
            REPLACE(REPLACE(event_data :from :: STRING, '['), ']'),
            ', '
        ) :: ARRAY AS from_teleport_array,
        COALESCE(
            event_data :hash,
            event_data :txHash
        ) :: STRING AS hash_teleport
    FROM
        events
    WHERE
        event_index = 0
        AND event_contract LIKE '%Teleport%'
        AND event_type IN (
            'TokensTeleportedIn',
            'Unlocked'
        )
        AND from_teleport :: STRING NOT LIKE '%{\"ArrayType%'
),
fees AS (
    SELECT
        tx_id,
        event_data :amount :: DOUBLE AS amount_fee,
        event_data :type :: NUMBER AS type_fee
    FROM
        events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                teleports
        )
        AND event_type = 'FeeCollected'
)
SELECT
    *,
    amount_teleport - amount_fee AS net_amount
FROM
    teleports
    LEFT JOIN fees USING (tx_id)
