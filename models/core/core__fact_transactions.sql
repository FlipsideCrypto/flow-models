{{
    config(
        materialized='view'
    )
}}

with txs as (

    select * from {{ ref('gold__transactions') }}

)

select * from txs