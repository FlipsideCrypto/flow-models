{{ config(
    materialized = 'incremental',
    unique_key = ['address', 'blockchain'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = 'modified_timestamp::DATE',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address, label_type, label_subtype, address_name, label), SUBSTRING(address, label_type, label_subtype, address_name, label); DELETE FROM {{ this }} WHERE address in (SELECT address FROM {{ ref('silver__labels') }} WHERE _is_deleted = TRUE);",
    tags = ['gold_labels']
) }}

SELECT
    blockchain,
    creator,
    address,
    address_name,
    label_type,
    label_subtype,
    project_name AS label,
    {{ dbt_utils.generate_surrogate_key(['labels_id']) }} AS dim_labels_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver_evm__labels') }} s 

{% if is_incremental() %}
WHERE
    s.modified_timestamp > (
        SELECT
            COALESCE(MAX(modified_timestamp), '1970-01-01' :: TIMESTAMP) AS modified_timestamp
        FROM
            {{ this }}
    )
{% endif %}