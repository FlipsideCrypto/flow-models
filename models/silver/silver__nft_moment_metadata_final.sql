{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp'],
    unique_key = "concat_ws('-',event_contract,edition_id,nft_id)",
    incremental_strategy = 'delete+insert',
    tags = ['nft', 'dapper']
) }}

WITH moments AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_minted') }}

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
metadata AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_metadata') }}

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
editions AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_editions') }}

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
series AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_series') }}

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
set_nm AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_set') }}

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),
FINAL AS (
    SELECT
        -- tx id and block timestamp don't matter for the final table
        m.tx_id,
        m.block_timestamp,
        m.event_contract,
        m.nft_id,
        m.serial_number,
        e.max_mint_size,
        e.play_id,
        e.series_id,
        s.series_name,
        e.set_id,
        sn.set_name,
        e.edition_id,
        e.tier,
        pl.metadata,
        m._inserted_timestamp
    FROM
        moments m
        LEFT JOIN editions e USING (
            event_contract,
            edition_id
        )
        LEFT JOIN metadata pl USING (
            event_contract,
            play_id
        )
        LEFT JOIN series s USING (
            event_contract,
            series_id
        )
        LEFT JOIN set_nm sn USING (
            event_contract,
            set_id
        )
)
SELECT
    *
FROM
    FINAL
