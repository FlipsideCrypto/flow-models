{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'event_contract',
    tags = ['scheduled_non_core','increment', 'dex']
) }}

WITH pair_data AS (
    SELECT 
        event_data:pairAddress::STRING AS pairAddress,
        'A.' || SUBSTR(pairAddress, 3, 99) || '.SwapPair' AS event_contract,
        event_data:token0Key::STRING AS token0_contract,
        event_data:token1Key::STRING AS token1_contract,
        modified_timestamp
    FROM 
        {{ ref('silver__streamline_events') }}
    WHERE 
        event_contract = 'A.b063c16cac85dbd1.SwapFactory'
        AND event_type = 'PairCreated'
        
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
)

SELECT 
    pairAddress,
    event_contract,
    token0_contract,
    token1_contract,
    -- Now we use the event_contract from the CTE which is guaranteed to be the unique pair contract
    {{ dbt_utils.generate_surrogate_key(['event_contract']) }} AS increment_deployed_pairs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM 
    pair_data