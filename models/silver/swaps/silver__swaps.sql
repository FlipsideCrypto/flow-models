{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = "CONCAT_WS('-', tx_id, swap_index)",
    incremental_strategy = 'delete+insert',
    tags = ['scheduled']
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__swaps_events') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
swap_events AS (
    SELECT
        *
    FROM
        events
    WHERE
        tx_id NOT IN (
            SELECT
                DISTINCT tx_id
            FROM
                events
            WHERE
                event_type = 'RewardTokensWithdrawn'
                OR event_type = 'NFTReceived'
        ) -- PierPair needs a bespoke model as it does not deposit traded token to Pool contract
        AND tx_id NOT IN (
            SELECT
                DISTINCT tx_id
            FROM
                events
            WHERE
                event_contract LIKE '%PierPair%'
        )
),
pool_info AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        event_index,
        event_type,
        RANK() over (
            PARTITION BY tx_id
            ORDER BY
                event_index
        ) - 1 AS swap_index,
        event_contract AS pool_contract,
        CASE
            WHEN object_keys(event_data) [0] :: STRING = 'side' THEN 'Blocto'
            WHEN object_keys(event_data) [0] :: STRING = 'direction' THEN 'Increment'
            WHEN object_keys(event_data) [3] :: STRING = 'swapAForB' THEN 'Metapier'
            ELSE 'Other'
        END AS likely_dex,
        COALESCE(
            event_data :direction :: NUMBER,
            event_data :side :: NUMBER - 1,
            event_data :swapAForB :: BOOLEAN :: NUMBER
        ) AS direction,
        COALESCE(
            event_data :inTokenAmount,
            event_data :token1Amount,
            event_data :amountIn
        ) :: DOUBLE AS in_token_amount,
        COALESCE(
            event_data :outTokenAmount,
            event_data :token2Amount,
            event_data :amountOut
        ) :: DOUBLE AS out_token_amount,
        _inserted_timestamp
    FROM
        swap_events
    WHERE
        event_type IN (
            'Trade',
            'Swap'
        )
),
token_withdraws AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        event_index,
        RANK() over (
            PARTITION BY tx_id
            ORDER BY
                event_index
        ) - 1 AS token_index,
        RANK() over (
            PARTITION BY CONCAT(
                tx_id,
                event_data :amount :: STRING,
                event_data :from :: STRING
            )
            ORDER BY
                event_index
        ) - 1 AS unique_order,
        event_contract,
        event_data,
        _inserted_timestamp
    FROM
        swap_events
    WHERE
        event_type = 'TokensWithdrawn'
        AND tx_id IN (
            SELECT
                DISTINCT tx_id
            FROM
                pool_info
        )
),
token_deposits AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        event_index,
        RANK() over (
            PARTITION BY tx_id
            ORDER BY
                event_index
        ) - 1 AS token_index,
        RANK() over (
            PARTITION BY CONCAT(
                tx_id,
                event_data :amount :: STRING,
                event_data :to :: STRING
            )
            ORDER BY
                event_index
        ) - 1 AS unique_order,
        event_contract,
        event_data,
        _inserted_timestamp
    FROM
        swap_events
    WHERE
        event_type = 'TokensDeposited'
        AND tx_id IN (
            SELECT
                DISTINCT tx_id
            FROM
                pool_info
        )
),
link_token_movement AS (
    SELECT
        w.tx_id,
        w.block_timestamp,
        w.block_height,
        w._inserted_timestamp,
        -- set transfer index based on execution via deposit, not withdraw, event
        RANK() over (
            PARTITION BY w.tx_id
            ORDER BY
                d.event_index
        ) - 1 AS transfer_index,
        w.event_data :from :: STRING AS withdraw_from,
        d.event_data :to :: STRING AS deposit_to,
        w.event_data :amount :: DOUBLE AS amount,
        w.event_contract AS token_contract,
        w.event_contract = d.event_contract AS contract_check
    FROM
        token_withdraws w
        LEFT JOIN token_deposits d
        ON w.tx_id = d.tx_id
        AND w.event_contract = d.event_contract
        AND w.event_data :amount :: STRING = d.event_data :amount :: STRING
        AND w.unique_order = d.unique_order
),
restructure AS (
    SELECT
        t.tx_id,
        t.transfer_index,
        p.swap_index,
        RANK() over (
            PARTITION BY t.tx_id,
            swap_index
            ORDER BY
                transfer_index
        ) - 1 AS token_position,
        t.withdraw_from,
        t.deposit_to,
        CONCAT('0x', SPLIT(pool_contract, '.') [1]) AS pool_address,
        sub.trader,
        ARRAYS_OVERLAP(ARRAY_CONSTRUCT(t.withdraw_from, t.deposit_to), ARRAY_CONSTRUCT(pool_address, sub.trader)) AS transfer_involve_pool_or_trader,
        t.amount,
        t.token_contract,
        p.pool_contract,
        p.direction,
        p.in_token_amount,
        p.out_token_amount
    FROM
        link_token_movement t
        LEFT JOIN pool_info p
        ON p.tx_id = t.tx_id
        AND (
            p.in_token_amount = t.amount -- blocto takes a 0.3% fee
            OR ROUND((p.in_token_amount / 0.997) - t.amount) = 0
            OR p.out_token_amount = t.amount
            OR ROUND((p.out_token_amount / 0.997) - t.amount) = 0
        )
        AND transfer_index >= swap_index
        LEFT JOIN (
            SELECT
                tx_id,
                withdraw_from AS trader
            FROM
                link_token_movement
            WHERE
                transfer_index = 0
        ) sub
        ON t.tx_id = sub.tx_id
    WHERE
        swap_index IS NOT NULL -- exclude the network fee token movement
        AND transfer_involve_pool_or_trader
),
-- there are some cases where the same token is transferred multiple times in a single swap
-- causing an error with object agg
excl_dup_transfer AS (
    SELECT
        tx_id,
        pool_contract,
        swap_index,
        CONCAT(
            'token',
            token_position
        ),
        COUNT(1) AS identical_transfer_count
    FROM
        restructure
    GROUP BY
        1,
        2,
        3,
        4
    HAVING
        identical_transfer_count > 1
),
pool_token_alignment AS (
    SELECT
        tx_id,
        pool_contract,
        swap_index,
        OBJECT_AGG(CONCAT('token', token_position), token_contract :: variant) AS tokens,
        OBJECT_AGG(CONCAT('amount', token_position), amount) AS amounts,
        OBJECT_AGG(CONCAT('from', token_position), withdraw_from :: variant) AS withdraws,
        OBJECT_AGG(CONCAT('to', token_position), deposit_to :: variant) AS deposits
    FROM
        restructure
    WHERE
        tx_id NOT IN (
            SELECT
                DISTINCT tx_id
            FROM
                excl_dup_transfer
        )
    GROUP BY
        1,
        2,
        3
),
boilerplate AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        _inserted_timestamp,
        withdraw_from AS trader
    FROM
        link_token_movement
    WHERE
        transfer_index = 0
        AND tx_id NOT IN (
            SELECT
                DISTINCT tx_id
            FROM
                excl_dup_transfer
        )
),
FINAL AS (
    SELECT
        tx_id,
        block_timestamp,
        block_height,
        pool_contract AS swap_contract,
        swap_index,
        trader,
        withdraws :from0 :: STRING AS token_out_source,
        tokens :token0 :: STRING AS token_out_contract,
        amounts :amount0 :: DOUBLE AS token_out_amount,
        deposits :to1 :: STRING AS token_in_destination,
        tokens :token1 :: STRING AS token_in_contract,
        amounts :amount1 :: DOUBLE AS token_in_amount,
        _inserted_timestamp
    FROM
        boilerplate
        LEFT JOIN pool_token_alignment USING (tx_id)
)
SELECT
    *
FROM
    FINAL
