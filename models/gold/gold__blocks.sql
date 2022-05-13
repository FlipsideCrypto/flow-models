{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        cluster_by=['block_timestamp::DATE'],
        unique_key='block_height'
    )
}}

with
silver_blocks as (

    select * from {{ ref('silver__blocks') }}

    {{ % if is_incremental() %}}

        WHERE _ingested_at::DATE >= CURRENT_DATE - 2

    {{ % endif %}}

),

gold_blocks as (


    select
        block_height,
        block_timestamp,
        network,
        chain_id,
        tx_count,
        id,
        parent_id
    from silver__blocks

)

select * from gold_blocks