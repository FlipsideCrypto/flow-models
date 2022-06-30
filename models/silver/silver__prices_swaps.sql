{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::date'],
    unique_key = "CONCAT_WS('-', tx_id, event_index)"
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
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
stable_out AS (
    SELECT
        block_timestamp,
        token_in_contract AS token_contract,
        token_out_amount / token_in_amount AS swap_price
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
        block_timestamp,
        token_out_contract AS token_contract,
        token_in_amount / token_out_amount AS swap_price
    FROM
        swaps
    WHERE
        token_in_contract IN (
            'A.cfdd90d4a00f7b5b.TeleportedTetherToken',
            'A.3c5959b568896393.FUSD',
            'A.b19436aae4d94622.FiatToken'
        )
),
tbl_union AS (
    SELECT
        *
    FROM
        stable_out
    UNION
    SELECT
        *
    FROM
        stable_in
)
SELECT
    *
FROM
    tbl_union
