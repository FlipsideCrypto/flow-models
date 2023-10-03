{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    cluster_by = "_inserted_timestamp::date",
    tags = ['core', 'streamline_scheduled']
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
blocks AS (
    SELECT
        *
    FROM
        {{ ref('silver__streamline_blocks') }}
),
FINAL AS (
    SELECT
        t.tx_id,
        t.block_number,
        b.block_timestamp,
        t.block_id,
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
        LEFT JOIN blocks b
        ON t.block_number = b.block_number
)
SELECT
    tx_id,
    block_timestamp,
    block_number AS block_height,
    gas_limit,
    payer,
    arguments,
    authorizers,
    envelope_signatures,
    payload_signatures,
    proposal_key,
    script,
    events,
    status,
    status_code,
    error_message,
    NOT status_code :: BOOLEAN AS tx_succeeded,
    _inserted_timestamp,
    _partition_by_block_id
FROM
    FINAL
