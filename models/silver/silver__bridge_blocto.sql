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
        ) :: STRING AS hash_teleport,
        _ingested_at
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
),
deposits AS (
    SELECT
        tx_id,
        event_contract,
        event_index,
        event_data :amount :: DOUBLE AS amount_deposits,
        ROW_NUMBER() over (
            PARTITION BY tx_id
            ORDER BY
                amount_deposits DESC
        ) AS rn,
        event_data :to :: STRING AS to_deposits
    FROM
        flow_dev.silver.events_final
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                teleports
        )
        AND event_type = 'TokensDeposited'
    ORDER BY
        tx_id,
        amount_deposits DESC
),
blocto_inbound AS (
    SELECT
        t.tx_id,
        t.block_timestamp,
        t.block_height,
        t.event_contract_teleport AS teleport_contract,
        d.event_contract AS token_contract,
        t.amount_teleport,
        f.amount_fee,
        d.amount_deposits AS net_amount,
        d.to_deposits AS flow_wallet_address,
        'inbound' AS direction,
        'blocto' AS bridge
    FROM
        teleports t
        LEFT JOIN deposits d USING (tx_id)
        LEFT JOIN fees f USING (tx_id)
    WHERE
        d.rn = 1
)
SELECT
    *
FROM
    blocto_inbound
