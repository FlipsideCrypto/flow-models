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
    WHERE
        TYPEOF(data) != 'NULL_VALUE'

        {% endset %}
        {% set max_partition_key = run_query(query) [0] [0] %}
        {% set must_fr = False %}
        {% do log(
            "max_partition_key: " ~ max_partition_key,
            info = True
        ) %}
        {% if max_partition_key == '' or max_partition_key is none %}
            {% do log("No recent data found. Using FR table.", info = True) %}
            {% set query %}

            SELECT
                MAX(partition_key)
            FROM
                {{ ref('bronze_api__FR_points_transfers') }}
            WHERE
                TYPEOF(data) != 'NULL_VALUE'

            {% endset%}

            {% set max_partition_key = run_query(query) [0] [0] %}
            {% set must_fr = True %}
            {% do log(
                "max_partition_key: " ~ max_partition_key,
                info = True
            ) %}
            {# {% do exceptions.raise_compiler_error("max_partition_key is not set. Aborting model execution.") %} #}
        {% endif %}
    {% endif %}

SELECT
    partition_key,
    TO_TIMESTAMP(partition_key) :: DATE AS request_date,
    VALUE :address :: STRING AS from_address,
    ZEROIFNULL(VALUE :array_index :: INTEGER) AS batch_index,
    DATA :batchId :: STRING AS batch_id,
    DATA :createdAt :: TIMESTAMP_NTZ AS created_at,
    DATA :secondsToFinalize :: INTEGER AS seconds_to_finalize,
    DATA :status :: STRING AS batch_status,
    DATA :transfers :: ARRAY AS batch_transfers,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['VALUE :address :: STRING', 'DATA :batchId :: STRING']
    ) }} AS points_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {% if must_fr %}
        {{ ref('bronze_api__FR_points_transfers') }}
    {% else %}
        {{ ref('bronze_api__points_transfers') }}
    {% endif %}
WHERE
    partition_key = {{ max_partition_key }}
