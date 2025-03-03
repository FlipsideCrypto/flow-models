{{ config(
    materialized = 'incremental',
    unique_key = ['address', 'blockchain'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = 'modified_timestamp::DATE',
    tags = ['silver_labels']
) }}

SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name,
    _is_deleted,
    {{ dbt_utils.generate_surrogate_key(['labels_combined_id']) }} AS labels_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze_evm__labels') }} b 

{% if is_incremental() %}
WHERE
    b.modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}