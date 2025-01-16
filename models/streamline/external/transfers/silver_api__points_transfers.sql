{{ config(
    materialized = 'table',
    unique_key = "points_transfers_id",
    cluster_by = ['created_at :: DATE', 'from_address'],
    post_hook = [ "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(from_address, to_address)" ],
    tags = ['streamline_non_core']
) }}



WITH points_transfers_raw AS (

    SELECT
        partition_key,
        request_date,
        DATA,
        _inserted_timestamp
    FROM
        {{ ref('silver_api__points_transfers_response') }}

),
flatten_batches AS (
    SELECT
        partition_key,
        request_date,
        _inserted_timestamp,
        DATA :address :: STRING AS from_address,
        A.index AS batch_index,
        A.value :createdAt :: TIMESTAMP_NTZ AS created_at,
        A.value :batchId :: STRING AS batch_id,
        A.value :status :: STRING AS batch_status,
        A.value :transfers :: ARRAY AS batch_transfers
    FROM
        points_transfers_raw,
        LATERAL FLATTEN(
            DATA :transfers :: ARRAY
        ) A

),
flatten_transfers AS (
    SELECT
        partition_key,
        request_date,
        created_at,
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
    created_at,
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

