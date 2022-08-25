{% macro run_bulk_get_nfl_allday_moments_metadata() %}
{% set sql %}


select streamline.udf_bulk_get_nfl_allday_moments_metadata()
where exists (
    select 1
    from streamline.allday_moments_metadata_needed
    limit 1 
)

{% endset %}

{% do run_query(sql) %}
{% endmacro %}