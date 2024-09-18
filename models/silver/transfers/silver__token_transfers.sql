{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::date', 'modified_timestamp::date'],
    unique_key = "token_transfers_id",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,from_address,to_address,token_contract);",
    tags = ['scheduled_non_core']
) }}

WITH events AS (

    SELECT
        block_height,
        block_timestamp,
        tx_id,
        tx_succeeded,
        event_index,
        event_type,
        event_contract,
        event_data,
        _inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__streamline_events') }}
    WHERE
        _partition_by_block_id >= 85000000
        AND block_height >= 85981726
        AND (
            (
                event_contract = 'A.f233dcee88fe0abe.FungibleToken'
                AND event_type IN (
                    'Withdrawn',
                    'Deposited'
                )
            )
            OR (
                -- no initial "Withdrawal" event if it's a new token mint
                event_type IN ('TokensMinted'))
            )

{% if is_incremental() %}
AND modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
withdrawn AS (
    SELECT
        tx_id,
        event_index,
        event_contract,
        event_data :amount :: STRING AS amount_adj,
        event_data :balanceAfter :: STRING AS balance_after_adj,
        event_data :from :: STRING AS from_address,
        event_data :fromUUID :: STRING AS from_uuid,
        event_data :type :: STRING AS token_type,
        event_data :withdrawnUUID :: STRING AS withdrawn_uuid,
        _inserted_timestamp
    FROM
        events
    WHERE
        event_type = 'Withdrawn'
),
deposited AS (
    SELECT
        tx_id,
        event_index,
        block_height,
        block_timestamp,
        event_contract,
        event_data :amount :: STRING AS amount_adj,
        event_data :balanceAfter :: STRING AS balance_after_adj,
        event_data :depositedUUID :: STRING AS deposited_uuid,
        event_data :to :: STRING AS to_address,
        event_data :toUUID :: STRING AS to_uuid,
        event_data :type :: STRING AS token_type,
        row_number() OVER (
            PARTITION BY tx_id
            ORDER BY
                event_index
        ) AS rn,
        tx_succeeded,
        _inserted_timestamp
    FROM
        events
    WHERE
        event_type = 'Deposited'
),
minted AS (
    SELECT
        tx_id,
        event_index,
        event_contract AS token_type,
        event_data :amount :: STRING AS amount_adj,
        COALESCE(
            event_data :from :: STRING,
            event_data :type :: STRING
        ) AS from_address,
        '-1' AS withdrawn_uuid,
        row_number() OVER (
            PARTITION BY tx_id
            ORDER BY
                event_index
        ) AS rn
    FROM
        events
    WHERE
        event_type = 'TokensMinted'
)
SELECT
    d.deposited_uuid,
    COALESCE(
        w2.withdrawn_uuid,
        w.withdrawn_uuid,
        m.withdrawn_uuid
    ) AS withdrawn_uuid_root,
    d.tx_id,
    d.block_height,
    d.block_timestamp,
    REGEXP_REPLACE(
        COALESCE(
            w2.token_type,
            w.token_type,
            m.token_type
        ),
        '\.Vault$',
        ''
    ) AS token_contract,
    COALESCE(
        w2.from_address,
        w.from_address,
        m.from_address
    ) AS from_address,
    d.to_address,
    COALESCE(
        w2.amount_adj,
        w.amount_adj,
        m.amount_adj
    ) AS amount_adj,
    COALESCE(
        w2.balance_after_adj,
        w.balance_after_adj
    ) AS from_address_balance_after,
    d.balance_after_adj AS to_address_balance_after,
    d.to_uuid = '0' AS is_fee_transfer,
    d.tx_succeeded,
    d._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['d.tx_id', 'd.deposited_uuid :: STRING', 'withdrawn_uuid_root :: STRING']
    ) }} AS token_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    deposited d
    LEFT JOIN withdrawn w
    ON d.tx_id = w.tx_id
    AND d.deposited_uuid = w.withdrawn_uuid
    LEFT JOIN withdrawn w2
    ON d.tx_id = w2.tx_id
    AND w.from_uuid = w2.withdrawn_uuid
    LEFT JOIN minted m
    ON d.tx_id = m.tx_id
    AND d.amount_adj = m.amount_adj
    AND d.rn = m.rn
