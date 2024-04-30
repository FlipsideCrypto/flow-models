{{ config (
    materialized = 'view',
    tags = ['scheduled']
) }}

WITH labels AS (

    SELECT
        system_created_at AS _system_created_at,
        insert_date AS _ingested_at,
        blockchain,
        address,
        creator,
        label_type,
        label_subtype,
        address_name,
        project_name,
        _is_deleted,
        labels_combined_id
    FROM
        {{ source(
            'silver_crosschain',
            'labels_combined'
        ) }}
    WHERE
        blockchain = 'flow'
)
SELECT
    *
FROM
    labels
