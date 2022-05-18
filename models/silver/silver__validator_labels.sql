{{ config(
    materialized = 'table',
    cluster_by = ['validator_address'],
    unique_key = 'validator_address'
) }}

SELECT
    address AS validator_address,
    address_name AS validator_type,
    project_name
FROM
    {{ ref('silver__labels') }}
WHERE
    label_type = 'operator'
ORDER BY
    1
