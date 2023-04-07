{{ config(
    severity = "error"
) }}

WITH successful_txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_succeeded
        AND LOWER(
            transaction_result :status :: STRING
        ) NOT IN (
            'expired',
            'pending'
        )
)
SELECT
    *
FROM
    successful_txs
WHERE
    ARRAY_SIZE(
        transaction_result :events
    ) = 0
