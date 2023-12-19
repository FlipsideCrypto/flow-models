{{ config(
    materialized = 'view'
) }}

SELECT
    *
FROM
    {{ source(
        'flow_bronze',
        'espn_nfl_athletes'
    ) }}
