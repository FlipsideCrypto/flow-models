{{ config(
    materialized = 'table',
    cluster_by = ['_inserted_timestamp'],
    unique_key = "concat_ws('-',event_contract,edition_id,nft_id)",
    tags = ['nft', 'dapper', 'scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH moments_minted AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_minted_s') }}
),
metadata AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_metadata_s') }}
),
editions AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_editions_s') }}
),
series AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_series_s') }}
),
nft_sets AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_set_s') }}
),
FINAL AS (
    SELECT
        m.tx_id,
        m.block_timestamp,
        m.event_contract,
        m.nft_id,
        m.serial_number,
        e.max_mint_size,
        COALESCE(
            m.play_id,
            e.play_id
        ) AS play_id,
        COALESCE(
            m.series_id,
            e.series_id,
            sn.series_id
        ) AS series_id,
        s.series_name,
        COALESCE(
            m.set_id,
            e.set_id
        ) AS set_id,
        sn.set_name,
        COALESCE(
            m.edition_id,
            e.edition_id
        ) AS edition_id,
        e.tier,
        pl.metadata,
        set_data,
        series_data,
        edition_data,
        m._inserted_timestamp,
        sn._inserted_timestamp AS _inserted_timestamp_set
    FROM
        moments_minted m
        LEFT JOIN editions e
        ON m.event_contract = e.event_contract
        AND m.edition_id = e.edition_id
        LEFT JOIN metadata pl
        ON m.event_contract = pl.event_contract
        AND COALESCE(
            m.play_id,
            e.play_id
        ) = pl.play_id
        LEFT JOIN nft_sets sn
        ON m.event_contract = sn.event_contract
        AND COALESCE(
            m.set_id,
            e.set_id
        ) = sn.set_id
        LEFT JOIN series s
        ON m.event_contract = s.event_contract
        AND COALESCE(
            m.series_id,
            e.series_id,
            sn.series_id
        ) = s.series_id

)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['event_contract','nft_id', 'play_id']
    ) }} AS nft_moment_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
