{{ config (
    materialized = 'view'
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
        project_name
    FROM
        {{ source(
            'crosschain',
            'address_labels'
        ) }}
    WHERE
        blockchain = 'flow'
)
SELECT
    *
FROM
    labels
