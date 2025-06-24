{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "ez_token_transfers_id",
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
        IFF(
            C.decimals IS NOT NULL
            AND price IS NOT NULL,
            ROUND(
                amount_precise * price,
                2
            ),
            NULL
        ) AS amount_usd,
        C.decimals,
        C.symbol,
        C.name,
        IFF(
            topic_0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
            'erc20',
            NULL
        ) AS token_standard,
        p.is_verified as token_is_verified,
        fact_event_logs_id AS ez_token_transfers_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp
    FROM
        {{ ref('core_evm__fact_event_logs') }}
        f
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON DATE_TRUNC(
            'hour',
            block_timestamp
        ) = HOUR
        AND token_address = contract_address
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
{% if is_incremental() %}
, heal_prices as (
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
    p0.is_verified as token_is_verified,
    t0.name,
    t0.symbol,
    t0.decimals,
    t0.raw_amount_precise,
    t0.raw_amount,
    IFF(
        t0.decimals IS NULL,
        NULL,
        utils.udf_decimal_adjust(
            t0.raw_amount_precise,
            t0.decimals
        )
    ) AS amount_precise_heal,
    amount_precise_heal :: FLOAT AS amount_heal,
    IFF(
        t0.decimals IS NOT NULL
        AND p0.price IS NOT NULL,
        ROUND(
            amount_heal * p0.price,
            2
        ),
        NULL
    ) AS amount_usd_heal,
    t0.origin_function_signature,
    t0.origin_from_address,
    t0.origin_to_address,
    t0.ez_token_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ this }}
    t0
    INNER JOIN {{ ref('price__ez_prices_hourly') }}
    p0
    ON DATE_TRUNC(
        'hour',
        t0.block_timestamp
    ) = HOUR
    AND t0.contract_address = p0.token_address
WHERE t0.amount_usd IS NULL AND t0.block_timestamp > dateadd('day', -31, SYSDATE())
),
heal_metadata as (
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
    t0.token_is_verified,
    COALESCE(t0.name, c0.name) AS name_heal,
    COALESCE(t0.symbol, c0.symbol) AS symbol_heal,
    COALESCE(t0.decimals, c0.decimals) AS decimals_heal,
    t0.raw_amount_precise,
    t0.raw_amount,
    t0.amount_precise,
    t0.amount,
    t0.amount_usd,
    t0.origin_function_signature,
    t0.origin_from_address,
    t0.origin_to_address,
    t0.ez_token_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM    
    {{ this }}
    t0
    INNER JOIN {{ ref('core_evm__dim_contracts') }}
    c0
    ON t0.contract_address = c0.address
    WHERE t0.block_timestamp > dateadd('day', -31, SYSDATE())
    and (
    (t0.symbol IS NULL AND c0.symbol IS NOT NULL) 
    OR (t0.name IS NULL AND c0.name IS NOT NULL)
    OR (t0.decimals IS NULL AND c0.decimals IS NOT NULL)
    ) 
)
{% endif %}
,
final AS (

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
    token_is_verified,
    NAME,
    symbol,
    decimals,
    raw_amount_precise,
    raw_amount,
    amount_precise,
    amount,
    amount_usd,
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
    *
FROM
    heal_prices
UNION ALL
SELECT
    *
FROM
    heal_metadata
{% endif %}
)
select 
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    event_index,
    from_address,
    to_address,
    contract_address,
    token_standard,
    token_is_verified,
    NAME,
    symbol,
    decimals,
    raw_amount_precise,
    raw_amount,
    amount_precise,
    amount,
    amount_usd,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    ez_token_transfers_id,
    inserted_timestamp,
    modified_timestamp
from final 
qualify row_number() over (partition by ez_token_transfers_id order by modified_timestamp desc, amount_usd desc nulls last) = 1