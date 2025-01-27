{{ config (
    materialized = 'incremental',
    unique_key = ["contract_address", "implementation_contract"],
    cluster_by = ["start_timestamp::date"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    tags = ['evm']
) }}

WITH base AS (

    SELECT
        from_address,
        to_address,
        MIN(block_number) AS start_block,
        MIN(block_timestamp) AS start_timestamp,
        MAX(inserted_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('core_evm__fact_traces') }}
    WHERE
        TYPE = 'DELEGATECALL'
        AND trace_succeeded
        AND tx_succeeded
        AND from_address != to_address -- exclude self-calls

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '4 hours'
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    ALL
),
create_id AS (
    SELECT
        from_address AS contract_address,
        to_address AS implementation_contract,
        start_block,
        start_timestamp,
        CONCAT(
            from_address,
            '-',
            to_address
        ) AS _id,
        _inserted_timestamp
    FROM
        base
),
heal AS (
    SELECT
        contract_address,
        implementation_contract,
        start_block,
        start_timestamp,
        _id,
        _inserted_timestamp
    FROM
        create_id

{% if is_incremental() %}
UNION ALL
SELECT
    contract_address,
    implementation_contract,
    start_block,
    start_timestamp,
    _id,
    _inserted_timestamp
FROM
    {{ this }}
    JOIN create_id USING (
        contract_address,
        implementation_contract
    )
{% endif %}
),
FINAL AS (
    SELECT
        contract_address,
        implementation_contract,
        start_block,
        start_timestamp,
        _id,
        _inserted_timestamp
    FROM
        heal qualify ROW_NUMBER() over (
            PARTITION BY contract_address,
            implementation_contract
            ORDER BY
                start_block ASC
        ) = 1
)
SELECT
    f.contract_address,
    f.implementation_contract,
    f.start_block,
    f.start_timestamp,
    f._id,
    f._inserted_timestamp,
    COALESCE(
        C.block_number,
        0
    ) AS created_block,
    COALESCE(
        p.block_number,
        0
    ) AS implementation_created_block
FROM
    FINAL f
    LEFT JOIN {{ ref('silver_evm__created_contracts') }} C
    ON f.contract_address = C.created_contract_address
    LEFT JOIN {{ ref('silver_evm__created_contracts') }}
    p
    ON f.implementation_contract = p.created_contract_address