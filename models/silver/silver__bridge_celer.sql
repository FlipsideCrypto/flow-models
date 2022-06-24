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
),
cbridge_txs AS (
    SELECT
        *
    FROM
        events
    WHERE
        event_contract = 'A.08dd120226ec2213.PegBridge'
),
inbound AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        event_contract AS bridge_contract,
        REPLACE(REPLACE(event_data :token :: STRING, '.Vault'), '"') AS token_contract,
        event_data :amount :: DOUBLE AS amount,
        event_data :receiver :: STRING AS flow_wallet_address,
        CONCAT(
            '0x',
            event_data :depositor
        ) :: STRING AS counterparty,
        event_data :refChId :: NUMBER AS chain_id,
        'inbound' AS direction,
        'cbridge' AS bridge,
        _ingested_at
    FROM
        events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                cbridge_txs
        )
        AND event_type = 'Mint'
),
outbound AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        event_contract AS bridge_contract,
        REPLACE(REPLACE(event_data :token :: STRING, '.Vault'), '"') AS token_contract,
        event_data :amount :: DOUBLE AS amount,
        event_data :burner :: STRING AS flow_wallet_address,
        event_data :toAddr :: STRING AS counterparty,
        event_data :toChain :: NUMBER AS chain_id,
        'outbound' AS direction,
        'cbridge' AS bridge,
        _ingested_at
    FROM
        events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                cbridge_txs
        )
        AND event_type = 'Burn'
),
tbl_union AS (
    SELECT
        *
    FROM
        inbound
    UNION
    SELECT
        *
    FROM
        outbound
)
SELECT
    *
FROM
    tbl_union
