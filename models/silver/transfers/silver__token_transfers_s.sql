{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::date', 'modified_timestamp::date'],
    unique_key = "CONCAT_WS('-', tx_id, sender, recipient, token_contract, amount)",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,sender,recipient,token_contract);",
    tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
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
        _partition_by_block_id,
        modified_timestamp
    FROM
        {{ ref('silver__streamline_events') }}
    WHERE
        -- crescendo upgrade cutoff
        _partition_by_block_id <= 86000000
        AND block_height < 85981726

{% if is_incremental() %}
AND
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
transfers AS (
    SELECT
        _inserted_timestamp,
        tx_id,
        event_contract,
        COUNT(event_type) AS event_count,
        MAX(
            event_index + 1
        ) AS max_index
    FROM
        events
    WHERE
        event_type IN (
            'TokensDeposited',
            'TokensWithdrawn',
            'FeesDeducted'
        )
    GROUP BY
        _inserted_timestamp,
        tx_id,
        event_contract
    HAVING
        event_count = max_index
        OR event_contract = 'A.b19436aae4d94622.FiatToken'
),
withdraws AS (
    SELECT
        block_height,
        _inserted_timestamp,
        block_timestamp,
        tx_id,
        CASE
            WHEN event_type = 'FiatTokenDeposited'
            AND event_contract = 'A.b19436aae4d94622.FiatToken' THEN event_data :to :: STRING
            ELSE event_data :from :: STRING
        END AS sender,
        event_contract AS token_contract,
        event_data :amount :: FLOAT AS amount,
        tx_succeeded
    FROM
        events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                transfers
        )
        AND (
            event_type IN (
                'TokensWithdrawn',
                'FiatTokenDeposited'
            )
        )
    GROUP BY
        block_height,
        _inserted_timestamp,
        block_timestamp,
        tx_id,
        sender,
        token_contract,
        amount,
        tx_succeeded
),
deposits AS (
    SELECT
        tx_id,
        _inserted_timestamp,
        event_data :to :: STRING AS recipient,
        event_contract AS token_contract,
        event_data :amount :: FLOAT AS amount
    FROM
        events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                transfers
        )
        AND event_type = 'TokensDeposited'
    GROUP BY
        tx_id,
        _inserted_timestamp,
        recipient,
        token_contract,
        amount
),
FINAL AS (
    SELECT
        block_height,
        w._inserted_timestamp AS _inserted_timestamp,
        block_timestamp,
        w.tx_id,
        sender,
        recipient,
        w.token_contract,
        SUM(COALESCE(d.amount, w.amount)) AS amount,
        tx_succeeded
    FROM
        withdraws w
        LEFT JOIN deposits d
        ON w.tx_id = d.tx_id
        AND w.token_contract = d.token_contract
        AND w.amount = d.amount
    WHERE
        sender IS NOT NULL
    GROUP BY
        block_height,
        w._inserted_timestamp,
        block_timestamp,
        w.tx_id,
        sender,
        recipient,
        w.token_contract,
        tx_succeeded
)
SELECT
    *,
    ROUND(
        block_height,
        -5
    ) AS _partition_by_block_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','sender', 'recipient','token_contract', 'amount']
    ) }} AS token_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
