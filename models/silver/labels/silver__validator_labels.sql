{{ config(
    materialized = 'table',
    tags = ['scheduled']
) }}

WITH labels AS (

    SELECT
        *
    FROM
        {{ ref('silver__labels') }}
    WHERE
        label_type = 'operator'
)
SELECT
    address AS node_id,
    address_name AS validator_type,
    project_name
FROM
    labels
