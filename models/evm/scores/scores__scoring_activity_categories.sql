{{ config (
    materialized = "view",
    tags = ['scores']
) }}

select * from {{ source('data_science_silver', 'scoring_activity_categories') }}