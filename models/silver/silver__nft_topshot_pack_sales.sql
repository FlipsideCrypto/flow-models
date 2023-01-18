{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = ['tx_id','nft_id'],
    tags = ['nft']
) }}

WITH silver_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__events_final') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
nft_txs AS (
    SELECT
        tx_id,
        event_index,
        block_height,
        block_timestamp,
        tx_succeeded,
        event_data :id :: INT AS nft_id,
        _inserted_timestamp
    FROM
        silver_events
    WHERE
        event_data :from = '0xe1f2a091f7bb5245'
        AND event_contract = 'A.0b2a3299cc857e29.TopShot'
)
SELECT
    A.tx_id,
    A.block_height,
    A.block_timestamp,
    'topshot pack' AS marketplace,
    NULL AS nft_collection,
    A.nft_id,
    b.event_data :to :: STRING buyer,
    NULL AS seller,
    NULL price,
    NULL currency,
    A.tx_succeeded,
    NULL AS tokenflow,
    NULL AS counterparties,
    MD5(
        CAST(COALESCE(CAST(A.tx_id AS VARCHAR), '') AS VARCHAR)
    ) AS pack_id,
    A._inserted_timestamp
FROM
    nft_txs A
    JOIN silver_events b
    ON A.tx_id = b.tx_id
    AND A.nft_id = b.event_data :id :: INT
WHERE
    event_data :to IS NOT NULL
    AND A.event_index <> b.event_index
