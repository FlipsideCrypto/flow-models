{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['left(season,4)'],
    unique_key = "nft_id",
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(nft_id,nbatopshot_id);",
    tags = ['scheduled_non_core'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT, TOPSHOT' }} }
) }}
-- depends_on: {{ ref('bronze__streamline_topshot_metadata') }}
WITH

{% if is_incremental() %}
{% else %}
    topshot_old AS (

        SELECT
            nft_id,
            nft_collection,
            nbatopshot_id,
            serial_number,
            total_circulation,
            moment_description,
            player,
            team,
            season,
            play_category,
            play_type,
            moment_date,
            set_name,
            set_series_number,
            video_urls,
            moment_stats_full,
            player_stats_game,
            player_stats_season_to_date,
            nft_moment_metadata_topshot_id AS dim_topshot_metadata_id
        FROM
            {{ ref('silver__nft_topshot_metadata_view') }}
    ),
{% endif %}

topshot AS (
    SELECT
        nft_id,
        nft_collection,
        nbatopshot_id,
        serial_number,
        total_circulation,
        moment_description,
        player,
        team,
        season,
        play_category,
        play_type,
        moment_date,
        set_name,
        set_series_number,
        video_urls,
        moment_stats_full,
        player_stats_game,
        player_stats_season_to_date,
        nft_topshot_metadata_v2_id AS dim_topshot_metadata_id
    FROM
        {{ ref('silver__nft_topshot_metadata_v2') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) modified_timestamp
        FROM
            {{ this }}
    )
{% endif %}
),
ua AS (
    SELECT
        nft_id,
        nft_collection,
        nbatopshot_id,
        serial_number,
        total_circulation,
        moment_description,
        player,
        team,
        season,
        play_category,
        play_type,
        moment_date,
        set_name,
        set_series_number,
        video_urls,
        moment_stats_full,
        player_stats_game,
        player_stats_season_to_date,
        dim_topshot_metadata_id
    FROM
        topshot

{% if is_incremental() %}
{% else %}
    UNION ALL
    SELECT
        nft_id,
        nft_collection,
        nbatopshot_id,
        serial_number,
        total_circulation,
        moment_description,
        player,
        team,
        season,
        play_category,
        play_type,
        moment_date,
        set_name,
        set_series_number,
        video_urls,
        moment_stats_full,
        player_stats_game,
        player_stats_season_to_date,
        dim_topshot_metadata_id
    FROM
        topshot_old
    {% endif %}
)
SELECT
    *,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    ua
