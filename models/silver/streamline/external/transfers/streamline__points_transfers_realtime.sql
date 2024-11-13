{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = '{{this.schema}}.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = {
            "external_table": "points_transfers",
            "sql_limit": "1",
            "producer_batch_size": "1",
            "worker_batch_size": "1",
            "sql_source": "{{this.identifier}}"
        }
    ),
    tags = ['streamline_non_core']
) }}


SELECT
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/points/dapp/transfer/all',
        {
            'Authorization': 'Bearer ' || '{Authentication}',
            'Accept': 'application/json',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'User-Agent': 'Flipside/0.1'
        },
        {},
        'Vault/prod/flow/points-api/prod'
    ) AS request

