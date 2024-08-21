{% macro run_create_udf_get_evm_chainhead() %}
    {% set sql %}

        CREATE 
            OR REPLACE FUNCTION {{ target.database }}.streamline.udf_get_evm_chainhead(
            network STRING DEFAULT 'testnet'
        )
        RETURNS INTEGER
        AS
        $$
            SELECT
            livequery.utils.udf_hex_to_int(
                flow.live.udf_api(
                    'POST',
                    CASE 
                        WHEN UPPER(network) = 'MAINNET' THEN 'https://mainnet.evm.nodes.onflow.org'
                        WHEN UPPER(network) = 'TESTNET' THEN 'https://testnet.evm.nodes.onflow.org'
                        ELSE NULL
                    END,
                    {},
                    OBJECT_CONSTRUCT(
                        'method', 'eth_blockNumber',
                        'id', 1,
                        'jsonrpc', '2.0',
                        'params', []
                    )
                ):data:result
            ) :: INTEGER AS block_number
        $$

     {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
