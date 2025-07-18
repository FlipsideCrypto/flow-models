{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['silver_bridge','defi','bridge','curated','scheduled_non_core']
) }}

WITH base_contracts AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address AS contract_address,
        concat_ws(
            '-',
            block_number,
            tx_position,
            CONCAT(
                TYPE,
                '_',
                trace_address
            )
        ) AS _call_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core_evm__fact_traces') }}
    WHERE
        from_address = '0x1d7c6783328c145393e84fb47a7f7c548f5ee28d'
        AND TYPE ILIKE 'create%'
        AND tx_succeeded
        AND trace_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND to_address NOT IN (
    SELECT
        DISTINCT pool_address
    FROM
        {{ this }}
)
{% endif %}
),
function_sigs AS (
    SELECT
        '0xfc0c546a' AS function_sig,
        'token' AS function_name
),
inputs AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        function_name,
        0 AS function_input,
        CONCAT(
            function_sig,
            LPAD(
                function_input,
                64,
                0
            )
        ) AS DATA
    FROM
        base_contracts
        JOIN function_sigs
        ON 1 = 1
),
contract_reads AS (
    SELECT
        contract_address,
        block_number,
        function_sig,
        function_name,
        function_input,
        DATA,
        utils.udf_json_rpc_call(
            'eth_call',
            [{ 'to': contract_address, 'from': null, 'data': data }, utils.udf_int_to_hex(block_number) ]
        ) AS rpc_request,
        live.udf_api(
            'POST',
            CONCAT(
                '{Service}',
                '/',
                '{Authentication}'
            ),{},
            rpc_request,
            'Vault/prod/flow/quicknode/mainnet'
        ) AS read_output,
        SYSDATE() AS _inserted_timestamp
    FROM
        inputs
),
reads_flat AS (
    SELECT
        read_output,
        read_output :data :id :: STRING AS read_id,
        read_output :data :result :: STRING AS read_result,
        SPLIT(
            read_id,
            '-'
        ) AS read_id_object,
        function_sig,
        function_name,
        function_input,
        DATA,
        contract_address,
        block_number,
        _inserted_timestamp
    FROM
        contract_reads
)
SELECT
    read_output,
    read_id,
    read_result,
    read_id_object,
    function_sig,
    function_name,
    function_input,
    DATA,
    block_number,
    contract_address AS pool_address,
    CONCAT('0x', SUBSTR(read_result, 27, 40)) AS token_address,
    _inserted_timestamp
FROM
    reads_flat
