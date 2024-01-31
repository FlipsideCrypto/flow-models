{{ config(
    materialized = 'incremental',
    unique_key = 'labels_id',
    cluster_by = 'modified_timestamp::DATE',
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(address); DELETE FROM {{ this }} WHERE _is_deleted = TRUE;",
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
        project_name,
        _is_deleted,
        labels_combined_id AS labels_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        {{ ref('bronze__labels') }}

    {% if is_incremental() %}
    WHERE modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
    {% endif %}
)
SELECT
    *
FROM
    labels
