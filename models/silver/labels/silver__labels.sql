{{ config(
    materialized = 'table',
    cluster_by = ['address'],
    unique_key = 'event_id',
    tags = ['scheduled', 'streamline_scheduled', 'scheduled_non_core']
) }}

WITH labels AS (

    SELECT
        _ingested_at as _inserted_timestamp,
        blockchain,
        address,
        creator,
        label_type,
        label_subtype,
        address_name,
        project_name
        {{ dbt_utils.generate_surrogate_key(
            ['event_id']
        ) }} AS labels_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id,
    FROM
        {{ ref('bronze__labels') }}
)
SELECT
    *
FROM
    labels
