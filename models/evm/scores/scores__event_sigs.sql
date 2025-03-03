{{ config (
    materialized = "view",
    tags = ['scores']
) }}

select * from {{ source('data_science_silver', 'evm_event_sigs') }}