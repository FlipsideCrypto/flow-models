{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp'],
    unique_key = "concat_ws('-',edition_id,nft_id)",
    incremental_strategy = 'delete+insert'
) }}

WITH moments AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_la_liga_moments_minted') }}

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
metadata AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_metadata') }}
    WHERE
        event_contract = 'A.87ca73a41bb50ad5.Golazos'

{% if is_incremental() %}
AND _inserted_timestamp >= (
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
        {{ ref('silver__nft_la_liga_editions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    m.tx_id,
    m.block_timestamp,
    m.event_contract,
    m.nft_id,
    m.serial_number,
    e.max_mint_size,
    e.play_id,
    e.series_id,
    e.set_id,
    e.edition_id,
    e.tier,
    pl.metadata,
    m._inserted_timestamp
FROM
    moments m
    LEFT JOIN editions e USING (edition_id)
    LEFT JOIN metadata pl
    ON (
        e.play_id = pl.play_id
    )
