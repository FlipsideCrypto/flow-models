-- depends_on: {{ ref('bronze_api__points_transfers') }}
-- depends_on: {{ ref('bronze_api__FR_points_transfers') }}
{{ config(
    materialized = 'incremental',
    unique_key = "batch_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp", "_inserted_timestamp"],
    cluster_by = ['modified_timestamp :: DATE', 'from_address'],
    post_hook = [ "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(from_address, to_address)" ],
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
        A.value :transfers :: ARRAY AS transfers
    FROM
        points_transfers_raw,
        LATERAL FLATTEN(DATA) A
),
flatten_batches AS (
    SELECT
        partition_key,
        request_date,
        _inserted_timestamp,
        address AS from_address,
        A.index AS batch_index,
        A.value :batchId :: STRING AS batch_id,
        A.value :status :: STRING AS batch_status,
        A.value :transfers :: ARRAY AS batch_transfers
    FROM
        flatten_protocols,
        LATERAL FLATTEN(
            transfers
        ) A
),
flatten_transfers AS (
    SELECT
        partition_key,
        request_date,
        from_address,
        batch_index,
        batch_id,
        _inserted_timestamp,
        A.index AS transfer_index,
        A.value :boxes :: NUMBER AS boxes,
        A.value :keys :: NUMBER AS keys,
        A.value :points :: NUMBER AS points,
        A.value :toAddressId :: STRING AS to_address
    FROM
        flatten_batches,
        LATERAL FLATTEN(batch_transfers) A
)
SELECT
    request_date,
    batch_id,
    batch_index,
    transfer_index,
    from_address,
    to_address,
    boxes,
    keys,
    points,
    partition_key,
    {{ dbt_utils.generate_surrogate_key(
        ['batch_id', 'transfer_index']
    ) }} AS points_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    _inserted_timestamp
FROM
    flatten_transfers 

qualify(ROW_NUMBER() over (PARTITION BY batch_id
ORDER BY
    _inserted_timestamp ASC)) = 1
