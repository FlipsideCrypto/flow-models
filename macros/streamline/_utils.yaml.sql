{% macro config_core__utils(schema="_utils") %}

- name: {{ schema }}.udf_register_secret
  signature:
    - [request_id, STRING]
    - [user_id, STRING]
    - [key, STRING]
  return_type: TEXT
  func_type: SECURE EXTERNAL
  api_integration: '{{ var("API_INTEGRATION") }}'
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
  sql: secret/register

- name: {{ schema }}.udf_whoami
  signature: []
  func_type: SECURE
  return_type: TEXT
  options: |
    NOT NULL
    RETURNS NULL ON NULL INPUT
    IMMUTABLE
    MEMOIZABLE
  sql: |
    SELECT
      COALESCE(PARSE_JSON(GETVARIABLE('LIVEQUERY_CONTEXT')):userId::STRING, CURRENT_USER())

{% endmacro %}