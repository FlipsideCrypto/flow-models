{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id',
    tags = ['scheduled', 'streamline_scheduled']
) }}

WITH silver_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_events') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    OR tx_id IN (
        SELECT
            tx_id
        FROM
            {{ this }}
        WHERE
            _inserted_timestamp >= SYSDATE() - INTERVAL '14 days'
            AND delegator IS NULL
    )
{% endif %}
),
flow_staking AS (
    SELECT
        tx_id,
        event_index,
        block_timestamp,
        block_height,
        tx_succeeded,
        event_contract,
        event_type AS action,
        event_data :amount :: FLOAT AS amount,
        event_data :delegatorID :: STRING AS delegator_id,
        event_data :nodeID :: STRING AS node_id,
        _inserted_timestamp
    FROM
        silver_events
    WHERE
        event_contract = 'A.8624b52f9ddcd04a.FlowIDTableStaking'
        AND event_type IN (
            'DelegatorTokensCommitted',
            'DelegatorRewardTokensWithdrawn',
            'DelegatorUnstakedTokensWithdrawn',
            'TokensCommitted',
            'RewardTokensWithdrawn',
            'UnstakedTokensWithdrawn'
        )
),
add_auth AS (
    SELECT
        tx_id,
        COALESCE(
            authorizers [1],
            authorizers [0]
        ) :: STRING AS primary_authorizer
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                flow_staking
        )

{% if is_incremental() %}
AND (
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    OR tx_id IN (
        SELECT
            tx_id
        FROM
            {{ this }}
        WHERE
            _inserted_timestamp >= SYSDATE() - INTERVAL '14 days'
            AND delegator IS NULL
    )
)
{% endif %}
),
FINAL AS (
    SELECT
        s.tx_id,
        event_index,
        block_timestamp,
        block_height,
        tx_succeeded,
        primary_authorizer AS delegator,
        action,
        amount,
        node_id,
        _inserted_timestamp
    FROM
        flow_staking s
        LEFT JOIN add_auth A USING (tx_id)
)
SELECT
    *
FROM
    FINAL
