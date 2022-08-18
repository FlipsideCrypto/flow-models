{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::date'],
    unique_key = 'tx_id'
) }}

WITH events AS (

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
cbridge_txs AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_index,
        event_contract,
        event_type,
        event_data,
        _ingested_at,
        _inserted_timestamp
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
        REPLACE(CONCAT('0x', event_data :depositor) :: STRING, '"') AS counterparty,
        event_data :refChId :: NUMBER AS chain_id,
        'inbound' AS direction,
        'cbridge' AS bridge,
        _ingested_at,
        _inserted_timestamp
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
        REPLACE(
            event_data :toAddr :: STRING,
            '"'
        ) AS counterparty,
        event_data :toChain :: NUMBER AS chain_id,
        'outbound' AS direction,
        'cbridge' AS bridge,
        _ingested_at,
        _inserted_timestamp
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
        tx_id,
        block_timestamp,
        block_height,
        bridge_contract,
        token_contract,
        amount,
        flow_wallet_address,
        counterparty,
        chain_id,
        direction,
        bridge,
        _ingested_at,
        _inserted_timestamp
    FROM
        inbound
    UNION
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        bridge_contract,
        token_contract,
        amount,
        flow_wallet_address,
        counterparty,
        chain_id,
        direction,
        bridge,
        _ingested_at,
        _inserted_timestamp
    FROM
        outbound
),
chain_ids AS (
    SELECT
        chain_id,
        blockchain
    FROM
        {{ ref('seeds__celer_chain_ids') }}
),
FINAL AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        bridge_contract,
        token_contract,
        amount,
        flow_wallet_address,
        counterparty,
        t.chain_id,
        l.blockchain,
        direction,
        bridge,
        _ingested_at,
        _inserted_timestamp
    FROM
        tbl_union t
        LEFT JOIN chain_ids l USING (chain_id)
)
SELECT
    *
FROM
    FINAL
