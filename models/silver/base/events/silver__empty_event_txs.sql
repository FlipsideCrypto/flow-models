{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['event_check', 'test']
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_succeeded = TRUE

{% if is_incremental() %}
AND _inserted_timestamp >= COALESCE(
    (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    ),
    '1900-01-01'
)
{% endif %}
),
events AS (
    SELECT
        DISTINCT tx_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__events_final') }}
    WHERE
        tx_succeeded = TRUE

{% if is_incremental() %}
AND _inserted_timestamp >= COALESCE(
    (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    ),
    '1900-01-01'
)
{% endif %}
),
diff AS (
    SELECT
        tx_id
    FROM
        txs
    EXCEPT
    SELECT
        tx_id
    FROM
        events
)
SELECT
    tx_id,
    block_height,
    block_timestamp,
    transaction_result,
    tx_succeeded,
    _inserted_timestamp,
    FALSE AS is_confirmed,
    FALSE AS is_api_error
FROM
    txs
WHERE
    tx_id IN (
        SELECT
            tx_id
        FROM
            diff
    )

{% if is_incremental() %}
AND _inserted_timestamp >= COALESCE(
    (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    ),
    '1900-01-01'
)
{% endif %}
