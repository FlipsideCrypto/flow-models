{{ config (
    materialized = 'incremental',
    unique_key = ["parent_contract_address","event_signature","start_block"],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['abis']
) }}

WITH new_abis AS (

    SELECT
        DISTINCT contract_address
    FROM
        {{ ref('silver_evm__flat_event_abis') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '18 hours'
        FROM
            {{ this }}
    )
UNION
    -- catches any late arriving implementation contracts - when we get its ABI but no delegatecalls where made yet
SELECT
    DISTINCT implementation_contract AS contract_address
FROM
    {{ ref('silver_evm__proxies') }}
WHERE
    start_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '18 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
implementations AS (
    SELECT
        p0.created_block,
        p0.implementation_created_block,
        p0.contract_address,
        p0.implementation_contract,
        p0.start_block,
        p0._id,
        p0._inserted_timestamp
    FROM
        {{ ref('silver_evm__proxies') }}
        p0
        JOIN new_abis na0
        ON p0.contract_address = na0.contract_address
    UNION
    SELECT
        p1.created_block,
        p1.implementation_created_block,
        p1.contract_address,
        p1.implementation_contract,
        p1.start_block,
        p1._id,
        p1._inserted_timestamp
    FROM
        {{ ref('silver_evm__proxies') }}
        p1
        JOIN new_abis na1
        ON p1.implementation_contract = na1.contract_address
),
all_relevant_contracts AS (
    SELECT
        DISTINCT contract_address
    FROM
        implementations
    UNION
    SELECT
        DISTINCT implementation_contract AS contract_address
    FROM
        implementations
    UNION
    SELECT
        contract_address
    FROM
        new_abis
),
flat_abis AS (
    SELECT
        contract_address,
        event_name,
        abi,
        simple_event_name,
        event_signature,
        NAME,
        inputs,
        event_type,
        _inserted_timestamp
    FROM
        {{ ref('silver_evm__flat_event_abis') }}
        JOIN all_relevant_contracts USING (contract_address)
),
base AS (
    SELECT
        ea.contract_address,
        event_name,
        abi,
        simple_event_name,
        event_signature,
        NAME,
        inputs,
        event_type,
        ea._inserted_timestamp,
        pb._inserted_timestamp AS implementation_inserted_timestamp,
        pb.start_block,
        pb.implementation_created_block,
        pb.contract_address AS base_contract_address,
        1 AS priority
    FROM
        flat_abis ea
        JOIN implementations pb
        ON ea.contract_address = pb.implementation_contract
    UNION ALL
    SELECT
        eab.contract_address,
        event_name,
        abi,
        simple_event_name,
        event_signature,
        NAME,
        inputs,
        event_type,
        eab._inserted_timestamp,
        pbb._inserted_timestamp AS implementation_inserted_timestamp,
        pbb.created_block AS start_block,
        pbb.implementation_created_block,
        pbb.contract_address AS base_contract_address,
        2 AS priority
    FROM
        flat_abis eab
        JOIN (
            SELECT
                DISTINCT contract_address,
                created_block,
                implementation_created_block,
                _inserted_timestamp
            FROM
                implementations
        ) pbb
        ON eab.contract_address = pbb.contract_address
    UNION ALL
    SELECT
        contract_address,
        event_name,
        abi,
        simple_event_name,
        event_signature,
        NAME,
        inputs,
        event_type,
        _inserted_timestamp,
        NULL AS implementation_inserted_timestamp,
        0 AS start_block,
        NULL AS implementation_created_block,
        contract_address AS base_contract_address,
        3 AS priority
    FROM
        flat_abis eac
    WHERE
        contract_address NOT IN (
            SELECT
                DISTINCT contract_address
            FROM
                implementations
        )
),
new_records AS (
    SELECT
        base_contract_address AS parent_contract_address,
        contract_address,
        event_name,
        abi,
        start_block,
        implementation_created_block,
        simple_event_name,
        event_signature,
        NAME,
        inputs,
        event_type,
        _inserted_timestamp,
        implementation_inserted_timestamp
    FROM
        base qualify ROW_NUMBER() over (
            PARTITION BY parent_contract_address,
            NAME,
            event_type,
            event_signature,
            start_block
            ORDER BY
                priority ASC,
                _inserted_timestamp DESC,
                implementation_created_block DESC nulls last,
                implementation_inserted_timestamp DESC nulls last
        ) = 1
),
FINAL AS (
    SELECT
        parent_contract_address,
        contract_address AS implementation_contract,
        event_name,
        abi,
        start_block,
        implementation_created_block,
        simple_event_name,
        event_signature,
        IFNULL(LEAD(start_block) over (PARTITION BY parent_contract_address, event_signature
    ORDER BY
        start_block) -1, 1e18) AS end_block,
        _inserted_timestamp,
        implementation_inserted_timestamp,
        SYSDATE() AS _updated_timestamp,
        {{ dbt_utils.generate_surrogate_key(
            ['parent_contract_address','event_signature','start_block']
        ) }} AS complete_event_abis_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        new_records qualify ROW_NUMBER() over (
            PARTITION BY parent_contract_address,
            event_name,
            event_signature,
            start_block
            ORDER BY
                _inserted_timestamp DESC
        ) = 1
)
SELECT
    parent_contract_address,
    implementation_contract,
    event_name,
    abi,
    start_block,
    implementation_created_block,
    simple_event_name,
    event_signature,
    end_block,
    _inserted_timestamp,
    implementation_inserted_timestamp,
    _updated_timestamp,
    complete_event_abis_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    FINAL f

{% if is_incremental() %}
LEFT JOIN {{ this }}
t USING (
    parent_contract_address,
    event_name,
    event_signature,
    start_block,
    end_block
)
WHERE
    t.event_signature IS NULL
{% endif %}