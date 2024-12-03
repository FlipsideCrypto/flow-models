-- depends_on: {{ ref('bronze_api__contract_abis') }}
-- depends_on: {{ ref('bronze_api__FR_contract_abis') }}
{{ config(
    materialized = 'incremental',
    unique_key = "contract_abis_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE'],
    tags = ['streamline_evm_non_core']
) }}

WITH bronze AS (

    SELECT
        VALUE :CONTRACT_ADDRESS :: STRING AS contract_address,
        partition_key,
        DATA :abi ::VARIANT as ABI,
        _inserted_timestamp
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
)
SELECT
    contract_address,
    ABI,
    partition_key,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['partition_key']
    ) }} AS contract_abis_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bronze
