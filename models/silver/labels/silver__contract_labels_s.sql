{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    cluster_by = ['event_contract'],
    unique_key = 'event_contract',
    tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH splt AS (

    SELECT
        event_contract,
        SPLIT(
            event_contract,
            '.'
        ) AS ec_s,
        _inserted_timestamp,
        _partition_by_block_id,
        modified_timestamp AS _modified_timestamp
    FROM
        {{ ref('silver__streamline_events') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
FINAL AS (
    SELECT
        DISTINCT event_contract,
        ec_s [array_size(ec_s)-1] :: STRING AS contract_name,
        CONCAT(
            '0x',
            ec_s [array_size(ec_s)-2] :: STRING
        ) AS account_address,
        _inserted_timestamp,
        _partition_by_block_id,
        _modified_timestamp
    FROM
        splt
    WHERE
        ec_s [0] != 'flow'
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['event_contract']
    ) }} AS event_contract_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify ROW_NUMBER() over (
        PARTITION BY event_contract
        ORDER BY
            _modified_timestamp DESC
    ) = 1
