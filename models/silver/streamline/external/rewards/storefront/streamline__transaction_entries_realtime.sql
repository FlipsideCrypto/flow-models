-- depends_on: {{ ref('silver_api__transaction_entries') }}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = '{{this.schema}}.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table": "transaction_entries",
        "sql_limit": "1000",
        "producer_batch_size": "1000",
        "worker_batch_size": "1000",
        "sql_source": "{{this.identifier}}" }
    ),
    tags = ['streamline_non_core']
) }}
{% if not var('INITIAL_RUN', false) %}
{% if execute %}
    {% set query %}

        SELECT
            entry_id
        FROM
            {{ ref('silver_api__transaction_entries') }}
        ORDER BY
            partition_key DESC,
            INDEX DESC
        LIMIT
            1 
    {% endset %}
    {% set last_id = run_query(query).columns [0].values() [0] %}
    {{ log("last_id: " ~ last_id, info=True) }}

    {% endif %}
{% endif %}

SELECT
    2 as api_limit,
    '{{ last_id }}' as starting_after,
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/api/loyalty/transaction_entries' || '?limit=' || api_limit{% if not var('INITIAL_RUN', false) %}|| '&startingAfter=' || '{{ last_id }}'{% endif %},
        { 'x-api-key': '{Authentication}' },
        {},
        'Vault/dev/flow/snag-api'
    ) AS request
