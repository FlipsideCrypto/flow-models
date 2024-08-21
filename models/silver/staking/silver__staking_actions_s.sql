{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    unique_key = 'tx_id',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,delegator);",
    tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH silver_events AS (

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
        _partition_by_block_id,
        modified_timestamp
    FROM
        {{ ref('silver__streamline_events') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    OR tx_id IN (
        SELECT
            tx_id
        FROM
            {{ this }}
        WHERE
            modified_timestamp >= SYSDATE() - INTERVAL '3 days'
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
        _inserted_timestamp,
        _partition_by_block_id
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
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    OR tx_id IN (
        SELECT
            tx_id
        FROM
            {{ this }}
        WHERE
            modified_timestamp >= SYSDATE() - INTERVAL '3 days'
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
        _inserted_timestamp,
        _partition_by_block_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id', 'event_index', 'action']
        ) }} AS staking_actions_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        flow_staking s
        LEFT JOIN add_auth A USING (tx_id)
)
SELECT
    *
FROM
    FINAL
