{{ config(
    materialized = 'table',
    cluster_by = ['_ingested_at::DATE'],
    unique_key = 'event_id',
) }}

WITH labels AS (

    SELECT
        _ingested_at,
        blockchain,
        address,
        creator label_type,
        label_subtype,
        address_name,
        project_name
    FROM
        {{ ref('bronze__labels') }}
)
SELECT
    *
FROM
    labels
