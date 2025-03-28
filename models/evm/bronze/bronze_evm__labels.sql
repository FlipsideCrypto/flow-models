{{ config(
    materialized = 'view',
    tags = ['bronze_labels']
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
    modified_timestamp,
    labels_combined_id
FROM
    {{ source(
        'crosschain_silver',
        'labels_combined'
    ) }}
WHERE
    blockchain = 'flow_evm'
    AND address LIKE '0x%'