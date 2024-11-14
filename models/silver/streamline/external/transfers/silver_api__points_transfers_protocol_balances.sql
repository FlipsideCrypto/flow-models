-- depends_on: {{ ref('bronze_api__points_transfers') }}
-- depends_on: {{ ref('bronze_api__FR_points_transfers') }}
{{ config(
    materialized = 'incremental',
    unique_key = "transfer_response_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp :: DATE'],
    tags = ['streamline_non_core']
) }}

WITH points_transfers_raw AS (

    SELECT
        partition_key,
        TO_TIMESTAMP(partition_key) :: DATE AS request_date,
        DATA,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze_api__points_transfers') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze_api__FR_points_transfers') }}
{% endif %}
),
flatten_protocols AS (
    SELECT
        partition_key,
        request_date,
        _inserted_timestamp,
        A.value :address :: STRING AS address,
        A.value :boxes :: NUMBER AS boxes,
        A.value :keys :: NUMBER AS keys,
        A.value :points :: NUMBER AS points,
        ARRAY_SIZE(
            A.value :transfers
        ) AS transfers_count,
        A.value :transfers :: ARRAY AS transfers
    FROM
        points_transfers_raw,
        LATERAL FLATTEN(DATA) A
)
SELECT
    partition_key,
    request_date,
    _inserted_timestamp,
    address,
    boxes,
    keys,
    points,
    transfers_count,
    transfers,
    {{ dbt_utils.generate_surrogate_key(
        ['partition_key', 'address']
    ) }} AS transfer_response_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_protocols 
qualify ROW_NUMBER() over (
        PARTITION BY transfer_response_id
        ORDER BY
            inserted_timestamp DESC
    ) = 1
