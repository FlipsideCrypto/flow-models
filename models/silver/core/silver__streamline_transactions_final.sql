{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    cluster_by = "_inserted_timestamp::date",
    tags = ['streamline_load', 'core']
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_transactions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
),
tx_results AS (
    SELECT
        *
    FROM
        {{ ref('silver__streamline_transaction_results') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
),
FINAL AS (
    SELECT
        t.block_number,
        t.block_id,
        t.tx_id,
        t.gas_limit,
        t.payer,
        t.arguments,
        t.authorizers,
        t.envelope_signatures,
        t.payload_signatures,
        t.proposal_key,
        t.script,
        tr.error_message,
        tr.events,
        tr.status,
        tr.status_code,
        LEAST(
            t._inserted_timestamp,
            tr._inserted_timestamp
        ) AS _inserted_timestamp,
        t._partition_by_block_id
    FROM
        txs t
        LEFT JOIN tx_results tr USING (tx_id)
)
SELECT
    *
FROM
    FINAL
