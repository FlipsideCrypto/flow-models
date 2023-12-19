{{ config(
    materialized = 'view'
) }}

SELECT
    ref,
    id,
    UID,
    guid,
    TYPE,
    alternate_ids,
    first_name,
    last_name,
    full_name,
    display_name,
    short_name,
    weight,
    display_weight,
    height,
    display_height,
    age,
    date_of_birth,
    links,
    birth_place,
    college,
    slug,
    headshot,
    jersey,
    POSITION,
    linked,
    team,
    statistics,
    contracts,
    experience,
    college_athlete,
    active,
    draft,
    status,
    statisticslog,
    debut_year,
    _inserted_timestamp :: timestamp_ntz AS inserted_timestamp
FROM
    {{ source(
        'flow_bronze',
        'espn_nfl_athletes'
    ) }}
