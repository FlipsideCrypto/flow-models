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
        tx_position,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) :: STRING AS from_address,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) :: STRING AS to_address,
        utils.udf_hex_to_int(SUBSTR(DATA, 3, 64)) AS raw_amount_precise,
        raw_amount_precise :: FLOAT AS raw_amount,
        IFF(
            C.decimals IS NULL,
            NULL,
            utils.udf_decimal_adjust(
                raw_amount_precise,
                C.decimals
            )
        ) AS amount_precise,
        amount_precise :: FLOAT AS amount,
        C.decimals,
        C.symbol,
        C.name,
        IFF(
            topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
            'erc20',
            NULL
        ) AS token_standard,
        fact_event_logs_id AS ez_token_transfers_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp
    FROM
        {{ ref('core_evm__fact_event_logs') }}
        f
        LEFT JOIN {{ ref('core_evm__dim_contracts') }} C
        ON contract_address = C.address
        AND (
            C.decimals IS NOT NULL
            OR C.symbol IS NOT NULL
            OR C.name IS NOT NULL
        )
    WHERE
        topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND tx_succeeded
        AND NOT event_removed
        AND topic_1 IS NOT NULL
        AND topic_2 IS NOT NULL
        AND DATA IS NOT NULL
        AND raw_amount IS NOT NULL

{% if is_incremental() %}
AND f.modified_timestamp > (
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
    event_index,
    from_address,
    to_address,
    contract_address,
    token_standard,
    NAME,
    symbol,
    decimals,
    raw_amount_precise,
    raw_amount,
    amount_precise,
    amount,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    ez_token_transfers_id,
    inserted_timestamp,
    modified_timestamp
FROM
    base

{% if is_incremental() %}
UNION ALL
SELECT
    t0.block_number,
    t0.block_timestamp,
    t0.tx_hash,
    t0.tx_position,
    t0.event_index,
    t0.from_address,
    t0.to_address,
    t0.contract_address,
    t0.token_standard,
    c0.name,
    c0.symbol,
    c0.decimals,
    t0.raw_amount_precise,
    t0.raw_amount,
    IFF(
        c0.decimals IS NULL,
        NULL,
        utils.udf_decimal_adjust(
            t0.raw_amount_precise,
            c0.decimals
        )
    ) AS amount_precise,
    amount_precise :: FLOAT AS amount,
    t0.origin_function_signature,
    t0.origin_from_address,
    t0.origin_to_address,
    t0.ez_token_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ this }}
    t0
    LEFT JOIN {{ ref('core_evm__dim_contracts') }}
    c0
    ON t0.contract_address = c0.address
    AND (
        c0.decimals IS NOT NULL
        OR c0.symbol IS NOT NULL
        OR c0.name IS NOT NULL
    )
    LEFT JOIN base b USING (ez_token_transfers_id)
WHERE
    b.ez_token_transfers_id IS NULL
    AND (
        t0.block_number IN (
            SELECT
                DISTINCT t1.block_number
            FROM
                {{ this }}
                t1
            WHERE
                t1.decimals IS NULL
                AND t1.modified_timestamp <= (
                    SELECT
                        MAX(modified_timestamp)
                    FROM
                        {{ this }}
                )
                AND EXISTS (
                    SELECT
                        1
                    FROM
                        {{ ref('core_evm__dim_contracts') }}
                        c1
                    WHERE
                        c1.modified_timestamp > DATEADD('DAY', -14, SYSDATE())
                        AND c1.decimals IS NOT NULL
                        AND t1.contract_address = c1.address)
                ) -- Only heal decimals if new data exists
                OR t0.block_number IN (
                    SELECT
                        DISTINCT t2.block_number
                    FROM
                        {{ this }}
                        t2
                    WHERE
                        t2.symbol IS NULL
                        AND t2.modified_timestamp <= (
                            SELECT
                                MAX(modified_timestamp)
                            FROM
                                {{ this }}
                        )
                        AND EXISTS (
                            SELECT
                                1
                            FROM
                                {{ ref('core_evm__dim_contracts') }}
                                c2
                            WHERE
                                c2.modified_timestamp > DATEADD('DAY', -14, SYSDATE())
                                AND c2.symbol IS NOT NULL
                                AND t2.contract_address = c2.address)
                        ) -- Only heal symbol if new data exists
                        OR t0.block_number IN (
                            SELECT
                                DISTINCT t3.block_number
                            FROM
                                {{ this }}
                                t3
                            WHERE
                                t3.name IS NULL
                                AND t3.modified_timestamp <= (
                                    SELECT
                                        MAX(modified_timestamp)
                                    FROM
                                        {{ this }}
                                )
                                AND EXISTS (
                                    SELECT
                                        1
                                    FROM
                                        {{ ref('core_evm__dim_contracts') }}
                                        c3
                                    WHERE
                                        c3.modified_timestamp > DATEADD('DAY', -14, SYSDATE())
                                        AND c3.name IS NOT NULL
                                        AND t3.contract_address = c3.address)
                                ) -- Only heal name if new data exists
        )
{% endif %}
