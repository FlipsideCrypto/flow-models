{% macro streamline_external_table_query(
        model,
        partition_function,
        partition_name,
        unique_key
    ) %}
    WITH meta AS (
        SELECT
            last_modified AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS {{ partition_name }}
        FROM
            TABLE(
                information_schema.external_table_file_registration_history(
                    start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                    table_name => '{{ source( "bronze_streamline", model) }}')
                ) A
            )
        SELECT
            block_number,
            {{ unique_key }},
            DATA,
            _inserted_timestamp,
            MD5(
                CAST(
                    COALESCE(CAST({{ unique_key }} AS text), '' :: STRING) AS text
                )
            ) AS _fsc_id,
            s.{{ partition_name }},
            s.value AS VALUE
        FROM
            {{ source(
                "bronze_streamline",
                model
            ) }}
            s
            JOIN meta b
            ON b.file_name = metadata$filename
            AND b.{{ partition_name }} = s.{{ partition_name }}
        WHERE
            b.{{ partition_name }} = s.{{ partition_name }}
{% endmacro %}

{% macro streamline_external_table_FR_query(
        model,
        partition_function,
        partition_name,
        unique_key
    ) %}
    WITH meta AS (
        SELECT
            registered_on AS _inserted_timestamp,
            file_name,
            {{ partition_function }} AS {{ partition_name }}
        FROM
            TABLE(
                information_schema.external_table_files(
                    table_name => '{{ source( "bronze_streamline", model) }}'
                )
            ) A
    )
    SELECT
        block_number,
        {{ unique_key }},
        DATA,
        _inserted_timestamp,
        MD5(
            CAST(
                COALESCE(CAST({{ unique_key }} AS text), '' :: STRING) AS text
            )
        ) AS _fsc_id,
        s.{{ partition_name }},
        s.value AS VALUE
    FROM
        {{ source(
            "bronze_streamline",
            model
        ) }}
        s
        JOIN meta b
        ON b.file_name = metadata$filename
        AND b.{{ partition_name }} = s.{{ partition_name }}
    WHERE
        b.{{ partition_name }} = s.{{ partition_name }}
{% endmacro %}


    {% macro streamline_multiple_external_table_query(
        table_names,
        partition_function,
        partition_name,
        unique_key
    )%}
    WITH 
    {% for table_name in table_names %}
        meta_{{ table_name }} AS (
            SELECT
                last_modified AS _inserted_timestamp,
                file_name,
                {{ partition_function }} AS {{ partition_name }}
            FROM
                TABLE(
                    information_schema.external_table_file_registration_history(
                        start_time => DATEADD('day', -3, CURRENT_TIMESTAMP()),
                        table_name => '{{ source( "bronze_streamline", table_name ) }}')
                    ) A
            ),
            {{ table_name }} AS (
                SELECT
                    block_number,
                    {{ unique_key }},
                    DATA,
                    _inserted_timestamp,
                    MD5(
                        CAST(
                            COALESCE(CAST(block_number AS text), '' :: STRING) AS text
                        )
                    ) AS _fsc_id,
                    s.{{ partition_name }},
                    s.value AS VALUE
                FROM
                    {{ source(
                        "bronze_streamline",
                        table_name
                    ) }}
                    s
                    JOIN meta_{{ table_name }}
                    b
                    ON b.file_name = metadata$filename
                    AND b.{{ partition_name }} = s.{{ partition_name }}
                WHERE
                    b.{{ partition_name }} = s.{{ partition_name }}
            ),
    {% endfor %}

    FINAL AS ({% for table_name in table_names %}
        SELECT
            *
        FROM
            {{ table_name }}

        {% if not loop.last %}
                UNION ALL
        {% endif %}
        {% endfor %}
    )
    SELECT
        *
    FROM
        FINAL

    {% endmacro %}
