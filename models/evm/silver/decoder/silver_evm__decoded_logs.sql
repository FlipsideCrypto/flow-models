-- depends_on: {{ ref('bronze__decoded_logs') }}
{{ config (
    materialized = "incremental",
    unique_key = "decoded_logs_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['modified_timestamp::date', 'round(block_number, -3)'],
    full_refresh = false,
    tags = ['evm_decoded_logs']
) }}

WITH base_data AS (

    SELECT
        block_number :: INTEGER AS block_number,
        SPLIT(id, '-')[0] :: STRING AS tx_hash,
        SPLIT(id, '-')[1] :: INTEGER AS event_index,
        DATA :name :: STRING AS event_name,
        LOWER(DATA :address :: STRING) :: STRING AS contract_address,
        DATA AS decoded_data,
        TO_TIMESTAMP_NTZ(_inserted_timestamp) AS _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__decoded_logs') }}
WHERE
    TO_TIMESTAMP_NTZ(_inserted_timestamp) >= (
        SELECT
            COALESCE(
                MAX(modified_timestamp), 
                '1900-01-01'::TIMESTAMP
            ) - INTERVAL '2 hours'
        FROM
            {{ this }}
    )
    AND is_object(DATA)
{% else %}
    {{ ref('bronze__decoded_logs_fr') }}
WHERE
    is_object(DATA)
{% endif %}

qualify(ROW_NUMBER() over (
    PARTITION BY
        block_number, event_index
    ORDER BY
        _inserted_timestamp DESC, _partition_by_created_date DESC)) = 1
),
transformed_logs AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        contract_address,
        event_name,
        decoded_data,
        _inserted_timestamp,
        utils.udf_transform_logs(decoded_data) AS transformed
    FROM
        base_data
),
FINAL AS (
    SELECT
        b.tx_hash,
        b.block_number,
        b.event_index,
        b.event_name,
        b.contract_address,
        b.decoded_data,
        transformed,
        b._inserted_timestamp,
        OBJECT_AGG(
            DISTINCT CASE
                WHEN v.value :name = '' THEN CONCAT('anonymous_', v.index)
                ELSE v.value :name
            END,
            v.value :value
        ) AS decoded_flat
    FROM
        transformed_logs b,
        LATERAL FLATTEN(
            input => transformed :data
        ) v
    GROUP BY
         ALL
)
SELECT 
    tx_hash,
    block_number,
    event_index,
    event_name,
    contract_address,
    decoded_data,
    transformed,
    decoded_flat,
    {{ dbt_utils.generate_surrogate_key(['block_number', 'event_index']) }} AS decoded_logs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM FINAL