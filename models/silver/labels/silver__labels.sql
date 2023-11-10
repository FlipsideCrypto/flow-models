{{ config(
    materialized = 'table',
    cluster_by = ['address'],
    unique_key = 'event_id',
    tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH labels AS (

    SELECT
        _ingested_at,
        blockchain,
        address,
        creator,
        label_type,
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
