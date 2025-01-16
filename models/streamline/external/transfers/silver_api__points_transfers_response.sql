-- depends_on: {{ ref('bronze_api__points_transfers') }}
-- depends_on: {{ ref('bronze_api__FR_points_transfers') }}
{{ config(
    materialized = 'table',
    unique_key = "points_transfers_id",
    tags = ['streamline_non_core']
) }}

{% if execute %}
    -- Query max partition key from the bronze table to use in CTE
    {% set query %}

    SELECT
        MAX(partition_key)
    FROM
        {{ ref('bronze_api__points_transfers') }}

        {% endset %}
        {% set max_partition_key = run_query(query) [0] [0] %}
        {% do log(
            "max_partition_key: " ~ max_partition_key,
            info = True
        ) %}
        {% if max_partition_key == '' or max_partition_key is none %}
            {% do exceptions.raise_compiler_error("max_partition_key is not set. Aborting model execution.") %}
        {% endif %}
    {% endif %}

SELECT
    partition_key,
    TO_TIMESTAMP(partition_key) :: DATE AS request_date,
    DATA,
    _inserted_timestamp,
    ROUND(OCTET_LENGTH(DATA) / 1048576, 2) AS data_mb,
    {{ dbt_utils.generate_surrogate_key(
        ['file_name', 'data :address :: STRING']
    ) }} AS points_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze_api__points_transfers') }}
WHERE
    partition_key = {{ max_partition_key }}
