{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_ingested_at::date'],
    unique_key = 'tx_id'
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
teleport_events AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        event_contract AS teleport_contract_fee,
        event_data :amount :: DOUBLE AS amount_fee,
        event_data :type :: NUMBER AS teleport_direction,
        _ingested_at
    FROM
        events
    WHERE
        event_type = 'FeeCollected'
        AND event_contract LIKE '%Teleport%'
),
teleports_in AS (
    SELECT
        tx_id,
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
        tx_id IN (
            SELECT
                tx_id
            FROM
                teleport_events
            WHERE
                teleport_direction = 1
        )
        AND event_index = 0
        AND event_contract LIKE '%Teleport%'
        AND event_type IN (
            'TokensTeleportedIn',
            'Unlocked'
        )
        AND from_teleport :: STRING NOT LIKE '%{\"ArrayType%'
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
        events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                teleports_in
        )
        AND event_type = 'TokensDeposited'
    ORDER BY
        tx_id,
        amount_deposits DESC
),
blocto_inbound AS (
    SELECT
        t.tx_id,
        f.block_timestamp,
        f.block_height,
        t.event_contract_teleport AS teleport_contract,
        d.event_contract AS token_contract,
        t.amount_teleport AS gross_amount,
        f.amount_fee,
        d.amount_deposits AS net_amount,
        d.to_deposits AS flow_wallet_address,
        f.teleport_direction,
        'blocto' AS bridge,
        f._ingested_at
    FROM
        teleports_in t
        LEFT JOIN deposits d USING (tx_id)
        LEFT JOIN teleport_events f USING (tx_id)
    WHERE
        d.rn = 1
),
teleports_out_withdraw AS (
    SELECT
        tx_id,
        event_contract,
        event_data :amount :: DOUBLE AS amount_withdraw,
        event_data :from :: STRING AS from_withdraw
    FROM
        events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                teleport_events
            WHERE
                teleport_direction = 0
        )
        AND event_index = 0
),
teleports_out AS (
    SELECT
        tx_id,
        event_contract AS event_contract_teleport,
        event_data :amount :: DOUBLE AS amount_teleport,
        event_data :to AS to_teleport,
        STRTOK_TO_ARRAY(
            REPLACE(REPLACE(event_data :to :: STRING, '['), ']'),
            ', '
        ) :: ARRAY AS to_teleport_array
    FROM
        events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                teleport_events
            WHERE
                teleport_direction = 0
        )
        AND event_type IN (
            'TokensTeleportedOut',
            'Locked'
        )
),
blocto_outbound AS (
    SELECT
        t.tx_id,
        f.block_timestamp,
        f.block_height,
        t.event_contract_teleport AS teleport_contract,
        w.event_contract AS token_contract,
        w.amount_withdraw AS gross_amount,
        f.amount_fee,
        t.amount_teleport AS net_amount,
        w.from_withdraw AS flow_wallet_address,
        f.teleport_direction,
        'blocto' AS bridge,
        f._ingested_at
    FROM
        teleports_out t
        LEFT JOIN teleports_out_withdraw w USING (tx_id)
        LEFT JOIN teleport_events f USING (tx_id)
),
tbl_union AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        teleport_contract,
        token_contract,
        gross_amount,
        amount_fee,
        net_amount,
        flow_wallet_address,
        teleport_direction,
        bridge,
        _ingested_at
    FROM
        blocto_inbound
    UNION
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        teleport_contract,
        token_contract,
        gross_amount,
        amount_fee,
        net_amount,
        flow_wallet_address,
        teleport_direction,
        bridge,
        _ingested_at
    FROM
        blocto_outbound
),
tele_labels AS (
    SELECT
        'A.04ee69443dedf0e4.TeleportCustody' AS teleport_contract,
        'Ethereum' AS blockchain
    UNION
    SELECT
        'A.0ac14a822e54cc4e.TeleportCustodyBSC' AS teleport_contract,
        'BSC' AS blockchain
    UNION
    SELECT
        'A.0ac14a822e54cc4e.TeleportCustodySolana' AS teleport_contract,
        'Solana' AS blockchain
    UNION
    SELECT
        'A.475755d2c9dccc3a.TeleportedSportiumToken' AS teleport_contract,
        'Ethereum' AS blockchain
    UNION
    SELECT
        'A.bd7e596b12e277df.TeleportCustody' AS teleport_contract,
        'Ethereum' AS blockchain
    UNION
    SELECT
        'A.c2fa71c36fd5b840.TeleportCustodyBSC' AS teleport_contract,
        'BSC' AS blockchain
    UNION
    SELECT
        'A.cfdd90d4a00f7b5b.TeleportedTetherToken' AS teleport_contract,
        'Ethereum' AS blockchain
),
FINAL AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        t.teleport_contract,
        token_contract,
        gross_amount,
        amount_fee,
        net_amount,
        flow_wallet_address,
        CASE
            WHEN teleport_direction = 0 THEN 'outbound'
            ELSE 'inbound'
        END AS teleport_direction,
        l.blockchain,
        bridge,
        _ingested_at
    FROM
        tbl_union t
        LEFT JOIN tele_labels l USING (teleport_contract)
)
SELECT
    *
FROM
    FINAL
