{% macro run_allday_metadata() %} 


{% set sql %}
    CALL {{ target.database }}.bronze_api.allday_metadata();
{% endset %}
    
    {% do run_query(sql) %}


{% endmacro %}