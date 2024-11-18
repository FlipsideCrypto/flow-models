{{ config(
    materialized = 'incremental',
    unique_key = 'evm_addresses_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['evm_addresses_id'],
    tags = ['streamline_non_core']
) }}

WITH evm_transactions AS (

    SELECT
        DISTINCT from_address AS address
    FROM
        {{ ref('silver_evm__transactions') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    address,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['address']
    ) }} AS evm_addresses_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evm_transactions 

qualify(ROW_NUMBER() over (PARTITION BY address
ORDER BY
    inserted_timestamp DESC)) = 1
