{{ config(
    materialized = "view",
    tags = ['streamline_realtime_evm']
) }}

{% if execute %}
    {% set height = run_query("SELECT streamline.udf_get_evm_chainhead()") %}
    {% set block_number = height.columns [0].values() [0] %}
{% else %}
    {% set block_number = 0 %}
{% endif %}

SELECT
    _id AS block_number
FROM
    {{ source(
        'silver_crosschain',
        'number_sequence'
    ) }}
WHERE
    _id <= {{ block_number }}
