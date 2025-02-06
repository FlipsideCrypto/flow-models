{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['evm']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        TYPE,
        trace_address,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        from_address,
        to_address,
        VALUE AS amount,
        value_precise_raw AS amount_precise_raw,
        value_precise AS amount_precise,
        ROUND(
            VALUE * price,
            2
        ) AS amount_usd,
        tx_position,
        trace_index,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'trace_index']
        ) }} AS ez_native_transfers_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp
    FROM
        {{ ref('core_evm__fact_traces') }}
        tr
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = hour
        AND is_native
    WHERE
        tr.value > 0
        AND tr.tx_succeeded
        AND tr.trace_succeeded
        AND tr.type NOT IN (
            'DELEGATECALL',
            'STATICCALL'
        )

{% if is_incremental() %}
AND tr.modified_timestamp > (
    SELECT
        COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    trace_index,
    trace_address,
    TYPE,
    from_address,
    to_address,
    amount,
    amount_precise_raw,
    amount_precise,
    amount_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    ez_native_transfers_id,
    inserted_timestamp,
    modified_timestamp
FROM
    base

{% if is_incremental() %}
UNION ALL
SELECT
    t.block_number,
    t.block_timestamp,
    t.tx_hash,
    t.tx_position,
    t.trace_index,
    t.trace_address,
    t.type,
    t.from_address,
    t.to_address,
    t.amount,
    t.amount_precise_raw,
    t.amount_precise,
    t.amount * p.price AS amount_usd_heal,
    t.origin_from_address,
    t.origin_to_address,
    t.origin_function_signature,
    t.ez_native_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ this }}
    t
    INNER JOIN {{ ref('price__ez_prices_hourly') }} p
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = hour
    AND p.is_native
    LEFT JOIN base b USING (ez_native_transfers_id)
WHERE
    t.amount_usd IS NULL
    AND t.block_timestamp :: DATE >= '2024-01-01'
    AND b.ez_native_transfers_id IS NULL
{% endif %}
