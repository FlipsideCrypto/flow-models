{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}


{% if execute %}
{% set height = run_query('SELECT streamline.udf_get_chainhead()') %}
{% set block_height = height.columns[0].values()[0] %}
{% else %}
{% set block_height = 0 %}
{% endif %}

SELECT
    height as block_height,
    node_url
FROM
    TABLE(streamline.udtf_get_base_table({{block_height}}))
WHERE
    block_height > 1065711 -- Root Height for Candidate node 4 
                           -- the earliest available block we can ingest 
                           -- https://developers.flow.com/concepts/nodes/node-operation/past-sporks#candidate-4
