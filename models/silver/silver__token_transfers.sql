{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::date'],
    unique_key = "CONCAT_WS('-', tx_id, sender, recipient, token_contract)"
) }}

WITH transfers AS (

SELECT
    _inserted_timestamp,
    tx_id,
    COUNT(event_type) AS event_count,
    MAX(event_index + 1) AS max_index
FROM 
    {{ ref('silver__events_final') }}
WHERE 
    event_type IN ('TokensDeposited', 'TokensWithdrawn', 'FeesDeducted')

{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY 
    _inserted_timestamp, tx_id
HAVING 
    event_count = max_index
),



withdraws AS (

SELECT
    block_height,
    _inserted_timestamp,
    block_timestamp,
    tx_id,
    event_data:from::STRING AS sender,
    event_contract AS token_contract,
    event_data:amount::FLOAT AS amount,
    tx_succeeded
FROM 
    {{ ref('silver__events_final') }}
WHERE 
    tx_id IN (SELECT tx_id FROM transfers)
AND
    event_type = 'TokensWithdrawn'
AND 
    block_timestamp::date >= '2022-04-20'
GROUP BY
    block_height, _inserted_timestamp, block_timestamp, tx_id, sender, token_contract, amount, tx_succeeded

  ),

deposits AS (

SELECT
    tx_id,
    _inserted_timestamp,
    event_data:to::STRING AS recipient,
    event_contract AS token_contract,
    event_data:amount::FLOAT AS amount
FROM 
    {{ ref('silver__events_final') }}
WHERE 
    tx_id IN (SELECT tx_id FROM transfers)
AND
    event_type = 'TokensDeposited'
AND 
    block_timestamp::date >= '2022-04-20'
GROUP BY 
    tx_id, _inserted_timestamp, recipient, token_contract, amount
  
  )
  
SELECT
    block_height,
    w._inserted_timestamp AS _inserted_timestamp,
    block_timestamp,
    w.tx_id,
    sender,
    recipient,
    w.token_contract,
    SUM(COALESCE(d.amount, w.amount)) AS amount,
    tx_succeeded
FROM 
    withdraws w
LEFT JOIN 
    deposits d
ON w.tx_id = d.tx_id
AND w.token_contract = d.token_contract
AND w.amount = d.amount
WHERE sender IS NOT NULL
GROUP BY 
    block_height, w._inserted_timestamp, block_timestamp, w.tx_id, sender, recipient, w.token_contract, tx_succeeded