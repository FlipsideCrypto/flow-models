{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::date'],
    unique_key = "CONCAT_WS('-', block_timestamp, token_contract)"
) }}

WITH swaps AS (

    SELECT
        tx_id,
        block_timestamp,
        block_height,
        swap_contract,
        trader,
        token_out_amount,
        token_out_contract,
        token_in_amount,
        token_in_contract,
        _ingested_at,
        _inserted_timestamp
    FROM
        {{ ref('silver__swaps_single_trade') }}

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
flow_price AS (
    SELECT
        DATE_TRUNC(
            'm',
            recorded_at
        ) AS _timestamp,
        price_usd
    FROM
        {{ ref('silver__prices') }}
    WHERE
        symbol = 'FLOW'
),
stable_out AS (
    SELECT
        tx_id,
        block_timestamp,
        token_in_contract AS token_contract,
        token_out_amount / token_in_amount AS swap_price,
        _inserted_timestamp,
        'stableswap' AS source
    FROM
        swaps
    WHERE
        token_out_contract IN (
            'A.cfdd90d4a00f7b5b.TeleportedTetherToken',
            'A.3c5959b568896393.FUSD',
            'A.b19436aae4d94622.FiatToken'
        )
),
stable_in AS (
    SELECT
        tx_id,
        block_timestamp,
        token_out_contract AS token_contract,
        token_in_amount / token_out_amount AS swap_price,
        _inserted_timestamp,
        'stableswap' AS source
    FROM
        swaps
    WHERE
        token_in_contract IN (
            'A.cfdd90d4a00f7b5b.TeleportedTetherToken',
            'A.3c5959b568896393.FUSD',
            'A.b19436aae4d94622.FiatToken'
        )
),
stbl_tbl_union AS (
    SELECT
        *
    FROM
        stable_out
    UNION
    SELECT
        *
    FROM
        stable_in
),
flow_out AS (
    SELECT
        tx_id,
        block_timestamp,
        token_in_contract AS token_contract,
        token_out_amount / token_in_amount AS swap_price_in_flow,
        _inserted_timestamp
    FROM
        swaps
    WHERE
        token_out_contract = 'A.1654653399040a61.FlowToken'
),
flow_in AS (
    SELECT
        tx_id,
        block_timestamp,
        token_in_contract AS token_contract,
        token_out_amount / token_in_amount AS swap_price_in_flow,
        _inserted_timestamp
    FROM
        swaps
    WHERE
        token_out_contract = 'A.1654653399040a61.FlowToken'
),
flow_tbl_union AS (
    SELECT
        tx_id,
        DATE_TRUNC(
            'm',
            block_timestamp
        ) AS _timestamp,
        token_contract,
        swap_price_in_flow,
        _inserted_timestamp,
        'flowswap' AS source
    FROM
        flow_out
    UNION
    SELECT
        tx_id,
        DATE_TRUNC(
            'm',
            block_timestamp
        ) AS _timestamp,
        token_contract,
        swap_price_in_flow,
        _inserted_timestamp,
        'flowswap' AS source
    FROM
        flow_in
),
to_usd AS (
    SELECT
        tx_id,
        ftu._timestamp,
        token_contract,
        swap_price_in_flow,
        swap_price_in_flow * p.price_usd AS swap_price_usd,
        _inserted_timestamp,
        source
    FROM
        flow_tbl_union ftu
        LEFT JOIN flow_price p USING (_timestamp)
),
FINAL AS (
    SELECT
        tx_id,
        block_timestamp,
        token_contract,
        swap_price,
        _inserted_timestamp,
        source
    FROM
        stbl_tbl_union
    UNION
    SELECT
        tx_id,
        _timestamp AS block_timestamp,
        token_contract,
        swap_price_usd AS swap_price,
        _inserted_timestamp,
        source
    FROM
        to_usd
    WHERE
        swap_price IS NOT NULL
)
SELECT
    *
FROM
    FINAL
