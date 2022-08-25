{% macro run_bulk_get_topshot_moments_metadata() %}
{% set sql %}


select streamline.udf_bulk_get_topshot_moments_minted_metadata()
where exists (
    select 1
    from streamline.all_topshot_moments_minted_metadata_needed
    limit 1 
)

{% endset %}

{% do run_query(sql) %}
{% endmacro %}