{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = "CONCAT_WS('-', tx_id, swap_index)",
    incremental_strategy = 'delete+insert'
) }}
-- TODO reminder the direction impacts if it is token in or out 
-- probably just a conditional, but still need to impldment

WITH swap_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__swaps_events') }}
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
        IFF(LOWER(object_keys(event_data) [0] :: STRING) = 'side', 'Blocto', 'Increment') AS likely_dex,
        COALESCE(
            event_data :direction :: NUMBER,
            event_data :side :: NUMBER - 1
        ) AS direction,
        COALESCE(
            event_data :inTokenAmount,
            event_data :token1Amount
        ) :: DOUBLE AS in_token_amount,
        COALESCE(
            event_data :outTokenAmount,
            event_data :token2Amount
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
        AND tx_id NOT IN (
            SELECT
                DISTINCT tx_id
            FROM
                swap_events
            WHERE
                event_type = 'RewardTokensWithdrawn'
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
        AND tx_id NOT IN (
            SELECT
                DISTINCT tx_id
            FROM
                swap_events
            WHERE
                event_type = 'RewardTokensWithdrawn'
        )
),
link_token_movement AS (
    SELECT
        w.tx_id,
        w.block_timestamp,
        w.block_height,
        w._inserted_timestamp,
        w.token_index,
        w.event_index AS event_index_w,
        d.event_index AS event_index_d,
        token_index AS transfer_index,
        w.event_data :from :: STRING AS withdraw_from,
        d.event_data :to :: STRING AS deposit_to,
        w.event_data :amount :: DOUBLE AS amount,
        w.event_contract AS token_contract,
        w.token_index = d.token_index AS token_check,
        w.event_contract = d.event_contract AS contract_check,
        w.event_data :amount :: DOUBLE = d.event_data :amount :: DOUBLE AS amount_check
    FROM
        token_withdraws w
        LEFT JOIN token_deposits d USING (
            tx_id,
            token_index,
            event_contract
        )
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
            p.in_token_amount = t.amount
            OR ROUND(
                p.in_token_amount / 0.997,
                3
            ) = ROUND(
                t.amount,
                3
            ) -- blocto takes a 0.3% fee out of the initial inToken
            OR p.out_token_amount = t.amount
            OR ROUND(
                p.out_token_amount / 0.997,
                3
            ) = ROUND(
                t.amount,
                3
            ) -- blocto takes a 0.3% fee out of the initial outToken
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
        tokens :token1 :: STRING AS token_in_destination,
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
