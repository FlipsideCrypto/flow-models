{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    cluster_by = "_inserted_timestamp::date",
    tags = ['core', 'streamline_scheduled', 'scheduled']
) }}

WITH retry_tx_ids AS (

    SELECT
        tx_id
    FROM
        {{ this }}
    WHERE
        (_inserted_timestamp >= SYSDATE() - INTERVAL '3 days'
        OR _inserted_timestamp IS NULL)
        AND (
            block_timestamp IS NULL
            OR pending_result_response
        )
),
txs AS (
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
    OR -- re-run record if block comes in later than tx records
    tx_id IN (
        SELECT
            tx_id
        FROM
            retry_tx_ids
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
    OR -- re-run record if block comes in later than tx records
    tx_id IN (
        SELECT
            tx_id
        FROM
            retry_tx_ids
    )
{% endif %}
),
blocks AS (
    SELECT
        *
    FROM
        {{ ref('silver__streamline_blocks') }}

{% if is_incremental() %}
WHERE
    DATE_TRUNC(
        'day',
        _inserted_timestamp
    ) >= (
        SELECT
            MAX(DATE_TRUNC('day', _inserted_timestamp))
        FROM
            {{ this }}
    ) - INTERVAL '24 hours'
{% endif %}
),
FINAL AS (
    SELECT
        COALESCE(
            t.tx_id,
            tr.tx_id
        ) AS tx_id,
        tr.status IS NULL AS pending_result_response,
        t.block_number,
        b.block_timestamp,
        t.gas_limit,
        CONCAT(
            '0x',
            payer
        ) AS payer,
        t.arguments,
        {{ target.database }}.silver.udf_address_array_adj(
            t.authorizers
        ) AS authorizers,
        ARRAY_SIZE(
            t.authorizers
        ) AS count_authorizers,
        t.envelope_signatures,
        t.payload_signatures,
        t.proposal_key,
        CONCAT(
            '0x',
            t.proposal_key: address :: STRING
        ) AS proposer,
        t.script,
        tr.error_message,
        tr.events,
        tr.status,
        tr.status_code,
        GREATEST(
            [b._inserted_timestamp],
            [tr._inserted_timestamp],
            [t._inserted_timestamp]
        ) [0] :: timestamp_ntz AS _inserted_timestamp,
        t._partition_by_block_id
    FROM
        txs t
        LEFT JOIN tx_results tr USING (tx_id)
        LEFT JOIN blocks b
        ON t.block_number = b.block_number
)
SELECT
    tx_id,
    pending_result_response,
    block_timestamp,
    block_number AS block_height,
    gas_limit,
    payer,
    arguments,
    authorizers,
    count_authorizers,
    envelope_signatures,
    payload_signatures,
    proposal_key,
    proposer,
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
