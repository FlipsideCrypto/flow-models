-- depends_on: {{ ref('silver__streamline_transactions') }}
{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    cluster_by = "_inserted_timestamp::date",
    tags = ['core', 'streamline_scheduled', 'scheduled', 'scheduled_core']
) }}

{% if execute %}
    {% set query = """
        CREATE OR REPLACE TEMPORARY TABLE silver.streamline_transactions_final_intermediate_tmp AS
            WITH retry_tx_ids AS (
                SELECT
                    tx_id,
                    block_height
                FROM """ ~ this ~ """
                WHERE
                    _inserted_timestamp >= SYSDATE() - INTERVAL '3 days'
                    AND (
                        block_timestamp IS NULL
                        OR pending_result_response
                    )
            )
            SELECT
                *
            FROM
                """ ~ ref('silver__streamline_transactions')
    %}
    {% set incr = "" %}
    {% if is_incremental() %}
        {% set incr = """
            WHERE
                _inserted_timestamp >= (
                    SELECT
                        MAX(_inserted_timestamp) _inserted_timestamp
                    FROM
                        """ ~ this ~ """
                )
                OR -- re-run record if block comes in later than tx records
                (
                    _inserted_timestamp >= SYSDATE() - INTERVAL '3 days'
                    AND
                    tx_id IN (
                        SELECT
                            tx_id
                        FROM
                            retry_tx_ids
                    )
                )
        """ %}
    {% endif %}

    {% set run = run_query(query ~ incr) %}
{% endif %}

/* 
   Do this because snowflake does not do well with dynamic query pruning. 
   This will set a "static" timestamp value which will always enable query pruning if the timestamp is a cluster key
   Coalesce in case there are 0 txs returned by the temp table
*/
{% if execute %}
{% set min_time = run_query("select coalesce(min(_inserted_timestamp),current_timestamp()) from silver.streamline_transactions_final_intermediate_tmp").columns[0].values()[0] %}
{% endif %}

WITH txs AS (
    SELECT
        *
    FROM
        silver.streamline_transactions_final_intermediate_tmp
),
tx_results AS (
    SELECT
        *
    FROM
        {{ ref('silver__streamline_transaction_results') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= '{{ min_time }}'
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
    ) - INTERVAL '3 days'
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
