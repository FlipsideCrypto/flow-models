{{ config (
    materialized = "incremental",
    unique_key = "contract_address",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(contract_address,abi_hash,bytecode), SUBSTRING(contract_address,abi_hash,bytecode)",
    tags = ['abis']
) }}

WITH verified_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        abi_source,
        2 as priority,
        discord_username,
        abi_hash
    FROM 
        {{ ref('silver_evm__verified_abis') }}
    WHERE
        abi_source = 'foundation'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        COALESCE(
            MAX(
                _inserted_timestamp
            ),
            '1970-01-01'
        )
    FROM
        {{ this }}
    WHERE
        abi_source = 'foundation'
)
{% endif %}
),
user_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        abi_source,
        discord_username,
        abi_hash,
        3 AS priority
    FROM
        {{ ref('silver_evm__verified_abis') }}
    WHERE
        abi_source = 'user'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        COALESCE(
            MAX(
                _inserted_timestamp
            ),
            '1970-01-01'
        )
    FROM
        {{ this }}
    WHERE
        abi_source = 'user'
)
{% endif %}
),
bytecode_abis AS (
    SELECT
        contract_address,
        abi AS DATA,
        _inserted_timestamp,
        'bytecode_matched' AS abi_source,
        NULL AS discord_username,
        abi_hash,
        4 AS priority
    FROM
        {{ ref('silver_evm__bytecode_abis') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(
                MAX(
                    _inserted_timestamp
                ),
                '1970-01-01'
            )
        FROM
        {{ this }}
    WHERE
        abi_source = 'bytecode_matched'
)
{% endif %}
),
all_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        abi_source,
        discord_username,
        abi_hash,
        priority
    FROM
        verified_abis
    UNION ALL
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        abi_source,
        discord_username,
        abi_hash,
        priority
    FROM
        user_abis
    UNION ALL
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        abi_source,
        discord_username,
        abi_hash,
        priority
    FROM
        bytecode_abis
),
priority_abis AS (
    SELECT
        contract_address,
        DATA,
        _inserted_timestamp,
        abi_source,
        discord_username,
        abi_hash,
        priority
    FROM
        all_abis qualify(ROW_NUMBER() over(PARTITION BY contract_address
    ORDER BY
        priority ASC)) = 1
)
SELECT
    p.contract_address,
    p.data,
    p._inserted_timestamp,
    p.abi_source,
    p.discord_username,
    p.abi_hash,
    created_contract_input AS bytecode,
    {{ dbt_utils.generate_surrogate_key(
        ['p.contract_address']
    ) }} AS abis_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    priority_abis p
    LEFT JOIN {{ ref('silver_evm__created_contracts') }}
    ON p.contract_address = created_contract_address
