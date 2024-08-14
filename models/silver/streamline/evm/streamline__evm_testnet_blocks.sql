{{ config (
    materialized = "view",
    tags = ['streamline_realtime_testnet']
) }}

{% if execute %}

{% set height = run_query("""
    select
    livequery.utils.udf_hex_to_int(
        flow.live.udf_api(
            'POST',
            'https://testnet.evm.nodes.onflow.org',
            {},
            object_construct(
                'method', 'eth_blockNumber',
                'id', 1,
                'jsonrpc', '2.0',
                'params', []
            )
        ):data:result
    ) as block_number
    """) %}
{% set block_height = height.columns[0].values()[0] %}
{% else %}
{% set block_height = 0 %}
{% endif %}

SELECT
    height as block_height
FROM
    TABLE(streamline.udtf_get_base_table({{block_height}}))
