{{ config(
    materialized = 'view'
) }}

SELECT
    ref,
    id,
    guid,
    UID,
    alternate_ids,
    slug,
    location,
    NAME,
    nickname,
    abbreviation,
    display_name,
    short_display_name,
    color,
    alternate_color,
    is_active,
    is_all_star,
    logos,
    RECORD,
    odds_records,
    athletes,
    venue,
    groups,
    ranks,
    statistics,
    leaders,
    links,
    injuries,
    notes,
    against_the_spread_records,
    franchise,
    depth_charts,
    projection,
    events,
    transactions,
    coaches,
    _inserted_timestamp :: timestamp_ntz AS inserted_timestamp
FROM
    {{ source(
        'flow_bronze',
        'espn_nfl_teams'
    ) }}
