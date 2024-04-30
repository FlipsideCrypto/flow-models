{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT, ALLDAY' }} },
    tag = ['scheduled']
) }}

WITH allday AS (

    SELECT
        NULL AS nft_unique_id,
        nft_id,
        nft_collection,
        nflallday_id,
        serial_number,
        moment_tier,
        total_circulation,
        moment_description,
        player,
        team,
        season,
        week,
        classification,
        play_type,
        moment_date,
        series,
        set_name,
        video_urls,
        moment_stats_full,
        _inserted_timestamp,
        _inserted_timestamp AS inserted_timestamp,
        _inserted_timestamp AS modified_timestamp
    FROM
        {{ ref('silver__nft_allday_metadata') }}
    UNION
    SELECT
        nft_allday_metadata_s_id AS nft_unique_id,
        nft_id,
        nft_collection,
        nflallday_id,
        serial_number,
        moment_tier,
        total_circulation,
        moment_description,
        player,
        team,
        season,
        week,
        classification,
        play_type,
        moment_date,
        series,
        set_name,
        video_urls,
        moment_stats_full,
        _inserted_timestamp,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__nft_allday_metadata_s') }}
    EXCEPT
    SELECT
        NULL AS nft_unique_id,
        nft_id,
        nft_collection,
        nflallday_id,
        serial_number,
        moment_tier,
        total_circulation,
        moment_description,
        player,
        team,
        season,
        week,
        classification,
        play_type,
        moment_date,
        series,
        set_name,
        video_urls,
        moment_stats_full,
        _inserted_timestamp,
        _inserted_timestamp AS inserted_timestamp,
        _inserted_timestamp AS modified_timestamp
    FROM
        {{ ref('silver__allday_moments_metadata_error') }}
)
SELECT
    nft_id,
    nft_collection,
    nflallday_id,
    serial_number,
    moment_tier,
    total_circulation,
    moment_description,
    player,
    team,
    season,
    week,
    classification,
    play_type,
    moment_date,
    series,
    set_name,
    video_urls,
    moment_stats_full,
    COALESCE (
        nft_unique_id,
        {{ dbt_utils.generate_surrogate_key(['nft_id']) }}
    ) AS dim_allday_metadata_id,
    COALESCE (
        inserted_timestamp,
        _inserted_timestamp
    ) AS inserted_timestamp,
    COALESCE (
        modified_timestamp,
        _inserted_timestamp
    ) AS modified_timestamp
FROM
    allday
