{{ config(
    materialized = 'incremental',
    unique_key = 'transaction_actors_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = 'block_timestamp::date',
    post_hook = 'ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,address);',
    tags = ['scheduled_core']
) }}

WITH transactions AS (

    SELECT
        tx_id,
        block_height,
        block_timestamp,
        authorizers,
        payer,
        proposer,
        events,
        _partition_by_block_id
    FROM
        {{ ref('silver__streamline_transactions_final') }}
    WHERE
        ARRAY_SIZE(events) > 0
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
flatten_events AS (
    SELECT
        tx_id,
        A.value :type :: STRING AS event_type,
        A.value :event_index :: INT AS event_index,
        COALESCE(
            b.value :value :value :type :: STRING,
            b.value :value :type :: STRING
        ) AS argument_type,
        b.value :name :: STRING AS argument_name,
        COALESCE(
            b.value :value :value :value :: STRING,
            b.value :value :value :: STRING
        ) AS address
    FROM
        transactions,
        LATERAL FLATTEN (events) A,
        LATERAL FLATTEN (
            VALUE :values :value :fields :: ARRAY
        ) b
    WHERE
        b.value :value :type :: STRING IN (
            'Optional',
            'Address'
        )
)
SELECT
    t.block_height,
    t.block_timestamp,
    A.tx_id,
    t.proposer,
    t.payer,
    t.authorizers,
    A.event_type,
    A.event_index,
    A.argument_name,
    A.address,
    t._partition_by_block_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'event_index', 'argument_name', 'address']
    ) }} AS transaction_actors_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_events A
    LEFT JOIN transactions t USING (tx_id)
WHERE
    A.argument_type = 'Address'
