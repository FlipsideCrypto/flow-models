{% macro run_create_udf_decode_hash_array() %}
    {% set sql %}

CREATE 
OR REPLACE FUNCTION {{ target.database }}.streamline.udf_decode_hash_array(raw_array ARRAY)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'decode_hash_array'
AS
$$
def decode_hash_array(raw_array):
    try:
        # Parse the JSON array
        data = raw_array
        
        # Extract and convert values
        hex_values = [format(int(item['value']), '02x') for item in data]
        
        # Concatenate and add prefix
        result = '0x' + ''.join(hex_values)
        
        return result.lower()
    except Exception as e:
        return f"Error: {str(e)}"
$$
;

     {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
