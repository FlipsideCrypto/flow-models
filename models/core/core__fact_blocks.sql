{{
    config(
        materialized='view'
    )
}}

with blocks as (

    select * from {{ ref('gold__blocks') }}

)

select * from blocks