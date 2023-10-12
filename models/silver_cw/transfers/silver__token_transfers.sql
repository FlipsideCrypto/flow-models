{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::date'],
    unique_key = "CONCAT_WS('-', tx_id, sender, recipient, token_contract, amount)",
    tags = ['scheduled', 'chainwalkers_scheduled']
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}
        -- WHERE
        --     event_data :: STRING != '{}'

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
        event_data :from :: STRING AS sender,
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
        AND event_type = 'TokensWithdrawn'
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
    *
FROM
    FINAL
