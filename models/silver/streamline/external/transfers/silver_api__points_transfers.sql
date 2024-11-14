-- depends_on: {{ ref('bronze_api__points_transfers') }}
-- depends_on: {{ ref('bronze_api__FR_points_transfers') }}
{{ config(
    materialized = 'incremental',
    unique_key = "points_transfers_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp :: DATE', 'from_address'],
    post_hook = [ "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(from_address)" ],
    tags = ['streamline_non_core']
) }}

WITH points_transfers_raw AS (

    SELECT
        partition_key,
        request_date,
        address,
        transfers,
        _inserted_timestamp
    FROM
        {{ ref('silver_api__points_transfers_protocol_balances') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
flatten_batches AS (
    SELECT
        partition_key,
        request_date,
        _inserted_timestamp,
        address AS from_address,
        b.index AS batch_index,
        b.value :batchId :: STRING AS batch_id,
        b.value :status :: STRING AS batch_status,
        b.value :transfers :: ARRAY AS transfers
    FROM
        points_transfers_raw,
        LATERAL FLATTEN(
            transfers
        ) b
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
        LATERAL FLATTEN(transfers) A
)
SELECT
    partition_key,
    request_date,
    from_address,
    batch_id,
    batch_index,
    transfer_index,
    boxes,
    keys,
    points,
    to_address,
    {{ dbt_utils.generate_surrogate_key(
        ['batch_id']
    ) }} AS points_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id,
    _inserted_timestamp
FROM
    flatten_transfers 
qualify(ROW_NUMBER() over (PARTITION BY points_transfers_id
ORDER BY
    _inserted_timestamp DESC)) = 1
