{% macro sp_create_bulk_get_topshot_moments_minted_metadata() %}
  {% set sql %}
  CREATE
  OR REPLACE PROCEDURE silver.sp_bulk_get_topshot_moments_minted_metadata() returns variant LANGUAGE SQL AS $$
DECLARE
  RESULT VARCHAR;
row_cnt INTEGER;
BEGIN
  row_cnt:= (
    SELECT
      COUNT(1)
    FROM
      {{ ref('silver__all_topshot_moments_minted_metadata_needed') }}
  );
if (
    row_cnt > 0
  ) THEN RESULT:= (
    SELECT
      silver.udf_bulk_get_topshot_moments_minted_metadata()
  );
  ELSE RESULT:= NULL;
END if;
RETURN RESULT;
END;$$ {% endset %}
{% do run_query(sql) %}
{% endmacro %}
