{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id',
    incremental_strategy = 'delete+insert',
    tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH swaps_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__swaps_events_s') }}

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
action_ct AS (
    SELECT
        tx_id,
        event_type,
        COUNT(1) AS n
    FROM
        swaps_events
    WHERE
        event_type IN (
            'Trade',
            'Swap'
        )
    GROUP BY
        1,
        2
),
step_ct AS (
    SELECT
        tx_id,
        OBJECT_AGG(
            event_type,
            n
        ) AS ob
    FROM
        action_ct
    GROUP BY
        1
),
single_trade AS (
    SELECT
        tx_id
    FROM
        step_ct
    WHERE
        ob :Trade = 1
        AND ob :Swap IS NULL
),
swaps_single_trade AS (
    SELECT
        *
    FROM
        swaps_events
    WHERE
        tx_id IN (
            SELECT
                tx_id
            FROM
                single_trade
        )
),
index_id AS (
    SELECT
        tx_id,
        event_type,
        MIN(event_index) AS event_index
    FROM
        swaps_single_trade
    WHERE
        event_type = 'TokensWithdrawn'
    GROUP BY
        1,
        2
),
token_out_data AS (
    SELECT
        sst.tx_id,
        block_timestamp,
        block_height,
        event_contract AS event_contract_token_out,
        event_data AS event_data_token_out,
        event_data :amount :: DOUBLE AS token_amount_token_out,
        LOWER(
            event_data :from :: STRING
        ) AS trader_token_out,
        _inserted_timestamp
    FROM
        index_id ii
        LEFT JOIN swaps_single_trade sst USING (
            tx_id,
            event_index
        )
),
trade_data AS (
    SELECT
        tx_id,
        block_timestamp,
        event_type,
        event_contract AS event_contract_trade,
        event_data AS event_data_trade,
        event_data :side :: NUMBER AS swap_side,
        event_data :token1Amount :: DOUBLE AS token_1_amount,
        -- note some are decimal adjusted, some are not. identify by contract
        event_data :token2Amount :: DOUBLE AS token_2_amount,
        l.account_address AS swap_account,
        _inserted_timestamp
    FROM
        swaps_single_trade sst
        LEFT JOIN {{ ref('silver__contract_labels') }}
        l USING (event_contract)
    WHERE
        event_type = 'Trade'
),
token_in_data AS (
    SELECT
        sst.tx_id,
        sst.block_timestamp,
        sst.event_contract AS event_contract_token_in,
        sst.event_data AS event_data_token_in,
        sst.event_data :amount :: DOUBLE AS amount_token_in
    FROM
        trade_data t
        LEFT JOIN swaps_single_trade sst
        ON sst.tx_id = t.tx_id
        AND t.swap_account = LOWER(
            sst.event_data :from :: STRING
        )
    WHERE
        sst.event_type = 'TokensWithdrawn'
),
combo AS (
    SELECT
        tod.tx_id,
        tod.block_timestamp,
        tod.block_height,
        td.event_contract_trade AS swap_contract,
        LOWER(
            tod.trader_token_out
        ) AS trader,
        tod.token_amount_token_out AS token_out_amount,
        tod.event_contract_token_out AS token_out_contract,
        tid.amount_token_in AS token_in_amount,
        tid.event_contract_token_in AS token_in_contract,
        -- keep these next 3 columns bc i can derive fees from the difference in token_out_amount and token_[n]_amount where n = swap_side
        td.swap_side,
        td.token_1_amount,
        td.token_2_amount,
        tod._inserted_timestamp
    FROM
        token_out_data tod
        LEFT JOIN trade_data td USING (tx_id)
        LEFT JOIN token_in_data tid USING (tx_id)
)
SELECT
    *
FROM
    combo
