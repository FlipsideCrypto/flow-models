{{ config(
    materialized = 'incremental',
    unique_key = 'tx_id',
    cluster_by = ['_inserted_timestamp::DATE']
) }}

WITH api_txs AS (

    SELECT
        tx_id,
        block_height,
        res,
        has_events,
        _request_timestamp AS _inserted_timestamp
    FROM
        {{ this.database }}.bronze_api.flow_api_check 
        qualify ROW_NUMBER() over (
            PARTITION BY tx_id
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
)
SELECT
    *
FROM
    api_txs
