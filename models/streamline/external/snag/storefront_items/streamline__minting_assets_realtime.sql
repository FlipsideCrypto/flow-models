 -- depends_on: {{ ref('silver_api__storefront_items') }}

{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = '{{this.schema}}.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table": "minting_assets",
        "sql_limit": "1",
        "producer_batch_size": "1",
        "worker_batch_size": "1",
        "sql_source": "{{this.identifier}}" }
    )
) }}

SELECT
    {{ var(
        'API_LIMIT',
        1000
    ) }} AS api_limit,
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/api/minting/assets' || '?organizationId=' || '{organization_id}' || '&websiteId=' || '{website_id}' || '&limit=' || api_limit || '&includeDeleted=true',
        { 'x-api-key': '{Authentication}' },
        {},
        'Vault/prod/flow/snag-api'
    ) AS request
