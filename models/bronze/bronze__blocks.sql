{{ config (
    materialized = 'view',
    tags = ['scheduled']
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
    ingested_at AS _ingested_at,
    _inserted_timestamp
FROM
    {{ source(
        'prod',
        'flow_blocks'
    ) }}
WHERE
    _inserted_timestamp :: DATE >= '2022-05-01'
