{{ config(
    materialized = 'incremental',
    cluster_by = ['_inserted_timestamp::DATE'],
    unique_key = 'tx_id',
    incremental_strategy = 'delete+insert',
    tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH events AS (

    SELECT
        *
    FROM
        {{ ref('silver__streamline_events') }}

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
pierpools AS (
    SELECT
        tx_id,
        block_timestamp,
        event_contract,
        event_type,
        event_data :poolId :: STRING AS pool_id,
        _inserted_timestamp
    FROM
        events
    WHERE
        event_contract = 'A.609e10301860b683.PierSwapFactory'
        AND event_type = 'NewPoolCreated'
),
pier_events AS (
    SELECT
        *
    FROM
        events
    WHERE
        tx_id IN (
            SELECT
                DISTINCT tx_id
            FROM
                pierpools
        )
),
token_withdraws AS (
    SELECT
        tx_id,
        block_timestamp,
        event_contract,
        event_index,
        _inserted_timestamp
    FROM
        pier_events
    WHERE
        event_type = 'TokensWithdrawn'
        AND event_data :amount :: DOUBLE = 0
),
pairs AS (
    SELECT
        tx_id,
        block_timestamp AS deployment_timestamp,
        event_contract AS token0_contract,
        LAG(event_contract) over (
            PARTITION BY tx_id
            ORDER BY
                event_index
        ) AS token1_contract,
        _inserted_timestamp
    FROM
        token_withdraws
),
escrow AS (
    SELECT
        tx_id,
        event_data :address :: STRING AS vault_address
    FROM
        pier_events
    WHERE
        event_contract = 'flow'
        AND event_type = 'AccountCreated'
),
pool_addr AS (
    SELECT
        tx_id,
        event_contract AS swap_contract
    FROM
        pier_events
    WHERE
        event_type = 'Mint'
),
FINAL AS (
    SELECT
        C.tx_id,
        C.deployment_timestamp,
        C.token0_contract,
        C.token1_contract,
        p.pool_id,
        e.vault_address,
        pa.swap_contract,
        C._inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }} AS labels_pools_metapier_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        pairs C
        LEFT JOIN pierpools p USING (tx_id)
        LEFT JOIN escrow e USING (tx_id)
        LEFT JOIN pool_addr pa USING (tx_id)
    WHERE
        token1_contract IS NOT NULL
)
SELECT
    *
FROM
    FINAL
