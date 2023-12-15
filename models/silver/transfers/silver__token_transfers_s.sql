{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::date'],
    unique_key = "CONCAT_WS('-', tx_id, sender, recipient, token_contract, amount)",
    tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_events') }}
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
usdc_withdraws AS (
    SELECT
        block_height,
        _inserted_timestamp,
        block_timestamp,
        tx_id,
        event_data :to :: STRING AS sender,
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
        AND event_contract = 'A.b19436aae4d94622.FiatToken'
        AND event_type in ('FiatTokenDeposited')
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
usdc_deposits AS (
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
        AND event_contract = 'A.b19436aae4d94622.FiatToken'
    GROUP BY
        tx_id,
        _inserted_timestamp,
        recipient,
        token_contract,
        amount
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
        AND event_type in ('TokensWithdrawn')
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
w_d AS (
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
),
usdc_w_d AS (
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
        usdc_withdraws w
        LEFT JOIN usdc_deposits d
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
),
FINAL AS
(
    SELECT 
        *
    FROM
        w_d
    UNION
    SELECT 
        *
    FROM
        usdc_w_d
)
SELECT
    * 
FROM
    FINAL