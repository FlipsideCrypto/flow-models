{% macro config_core_live(schema="live") %}

- name: {{ schema }}.udf_api
  signature:
    - [method, STRING]
    - [url, STRING]
    - [headers, OBJECT]
    - [data, OBJECT]
    - [secret_name, STRING]
  return_type: VARIANT
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
  sql: |
    SELECT
      _live.UDF_API(
          method,
          url,
          headers,
          data,
          _utils.UDF_WHOAMI(),
          secret_name
      )
- name: {{ schema }}.udf_api
  signature:
    - [method, STRING]
    - [url, STRING]
    - [headers, OBJECT]
    - [data, OBJECT]
  return_type: VARIANT
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
  sql: |
    SELECT
      _live.UDF_API(
          method,
          url,
          headers,
          data,
          _utils.UDF_WHOAMI(),
          ''
      )
- name: {{ schema }}.udf_api
  signature:
    - [url, STRING]
    - [data, OBJECT]
  return_type: VARIANT
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
  sql: |
    SELECT
      _live.UDF_API(
          'POST',
          url,
          {'Content-Type': 'application/json'},
          data,
          _utils.UDF_WHOAMI(),
          ''
      )
- name: {{ schema }}.udf_api
  signature:
    - [url, STRING]
    - [data, OBJECT]
    - [secret_name, STRING]
  return_type: VARIANT
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
  sql: |
    SELECT
      _live.UDF_API(
          'POST',
          url,
          {'Content-Type': 'application/json'},
          data,
          _utils.UDF_WHOAMI(),
          secret_name
      )
- name: {{ schema }}.udf_api
  signature:
    - [url, STRING]
  return_type: VARIANT
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    VOLATILE
  sql: |
    SELECT
      _live.UDF_API(
          'GET',
          url,
          {},
          {},
          _utils.UDF_WHOAMI(),
          ''
      )
      
{% endmacro %}