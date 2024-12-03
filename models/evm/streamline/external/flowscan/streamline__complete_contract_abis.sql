-- depends_on: {{ ref('bronze_api__contract_abis') }}
-- depends_on: {{ ref('bronze_api__FR_contract_abis') }}

{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['streamline_evm_non_core']
) }}

SELECT
    VALUE :CONTRACT_ADDRESS :: STRING AS contract_address,
    partition_key,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['contract_address']
    ) }} AS complete_contract_abis_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze_api__contract_abis') }}
WHERE
    _inserted_timestamp >= COALESCE(
        (
            SELECT
                MAX(_inserted_timestamp) _inserted_timestamp
            FROM
                {{ this }}
        ),
        '1900-01-01' :: timestamp_ntz
    )
{% else %}
    {{ ref('bronze_api__FR_contract_abis') }}
{% endif %}

-- TODO - check for valid results only

qualify(ROW_NUMBER() over (PARTITION BY contract_address
ORDER BY
    _inserted_timestamp DESC)) = 1
