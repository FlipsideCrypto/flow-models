-- depends_on: {{ ref('silver__testnet_transactions') }}
-- depends_on: {{ ref('silver__testnet_transaction_results') }}

{{ config(
    materialized = 'incremental',
    unique_key = "testnet_transaction_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = "block_timestamp::date",
    tags = ['testnet']
) }}

WITH txs AS (

    SELECT
        *
    FROM
        {{ ref('silver__testnet_transactions') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= SYSDATE() - INTERVAL '3 days'
{% endif %}
),
tx_results AS (
    SELECT
        *
    FROM
        {{ ref('silver__testnet_transaction_results') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= SYSDATE() - INTERVAL '3 days'
    AND tx_id IN (
        SELECT
            DISTINCT tx_id
        FROM
            txs
    )
{% endif %}

),
blocks AS (
    SELECT
        *
    FROM
        {{ ref('silver__testnet_blocks') }}


{% if is_incremental() %}
WHERE
    modified_timestamp >= SYSDATE() - INTERVAL '3 days'
    AND block_number IN (
        SELECT
            DISTINCT block_number
        FROM
            txs
    )
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
        ARRAY_SIZE(
            tr.events
        ) AS events_count,
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
    events_count,
    status,
    status_code,
    error_message,
    NOT status_code :: BOOLEAN AS tx_succeeded,
    _inserted_timestamp,
    _partition_by_block_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS testnet_transaction_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id 
FROM
    FINAL
