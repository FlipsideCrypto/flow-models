{{ config(
    materialized = 'table',
    cluster_by = ['_inserted_timestamp'],
    unique_key = "concat_ws('-',event_contract,edition_id,nft_id)",
    tags = ['nft', 'dapper', 'scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH moments AS (

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
set_nm AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_moment_set_s') }}
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
        m._inserted_timestamp,
        sn._inserted_timestamp AS _inserted_timestamp_set
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
    *,
    {{ dbt_utils.generate_surrogate_key(
            ['event_contract','edition_id','nft_id']
        ) }} AS nft_moment_metadata_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
