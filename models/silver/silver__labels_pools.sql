{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id',
    incremental_strategy = 'delete+insert'
) }}

WITH events AS (

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
pair_labels AS (
    SELECT
        *
    FROM
        {{ ref('silver__contract_labels') }}
    WHERE
        contract_name ILIKE '%swappair%'
),
pair_creation AS (
    SELECT
        tx_id,
        block_timestamp,
        event_contract,
        event_data :numPairs :: NUMBER AS pair_number,
        event_data :pairAddress :: STRING AS account_address,
        event_data :token0Key :: STRING AS token0_contract,
        event_data :token1Key :: STRING AS token1_contract,
        _inserted_timestamp
    FROM
        events
    WHERE
        event_type = 'PairCreated'
),
FINAL AS (
    SELECT
        tx_id,
        block_timestamp AS deployment_timestamp,
        pair_number as pool_id,
        p.account_address as vault_address,
        token0_contract,
        token1_contract,
        l.event_contract AS swap_contract,
        _inserted_timestamp
    FROM
        pair_creation p
        LEFT JOIN pair_labels l USING (account_address)
)
SELECT
    *
FROM
    FINAL
