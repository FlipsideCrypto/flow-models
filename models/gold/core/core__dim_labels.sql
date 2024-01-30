{{ config(
    materialized = 'view',
    tags = ['scheduled']
) }}

SELECT
    blockchain,
    creator,
    address,
    address_name,
    label_type,
    label_subtype,
    project_name AS label,
    COALESCE(
        labels_id,
        {{ dbt_utils.generate_surrogate_key(
            ['address']
        ) }}
    ) AS dim_labels_id,
    COALESCE(inserted_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as inserted_timestamp,
    COALESCE(modified_timestamp, '2000-01-01' :: TIMESTAMP_NTZ) as modified_timestamp
FROM
    {{ ref('silver__labels') }}
