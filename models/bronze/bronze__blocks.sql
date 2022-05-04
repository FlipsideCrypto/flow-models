{{ config (
    materialized = 'view'
) }}

SELECT 
    record_id, 
    offset_id,
    block_id,
    block_timestamp, 
    network, 
    chain_id, 
    tx_count, 
    header, 
    ingested_at as _ingested_at
FROM 
    {{ source(
      'prod',
      'flow_blocks'
    ) }} 