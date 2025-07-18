-- depends_on: {{ ref('silver_api__transaction_entries') }}
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = '{{this.schema}}.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table": "transaction_entries",
        "sql_limit": "1",
        "producer_batch_size": "1",
        "worker_batch_size": "1",
        "sql_source": "{{this.identifier}}" }
    )
) }}

{% if not var(
        'STOREFRONT_INITIAL_RUN',
        false
    ) %}
    {% if execute %}
        {% set query %}
            WITH target_entry_id AS (

                SELECT
                    entry_id,
                    ROW_NUMBER() over (
                        ORDER BY
                            partition_key DESC,
                            INDEX ASC
                    ) AS rn
                FROM
                    {{ ref('silver_api__transaction_entries') }}
                    WHERE _inserted_timestamp >= SYSDATE() - INTERVAL '3 days'
            )
            SELECT
                entry_id
            FROM
                target_entry_id
            WHERE
            rn = 2 
        {% endset %}
        {% set starting_after = run_query(query).columns [0].values() [0] %}
        {{ log(
            "last_id: " ~ starting_after,
            info = True
        ) }}
    {% endif %}
{% endif %}

SELECT
    {{ var(
        'API_LIMIT',
        1000
    ) }} AS api_limit,
    '{{ starting_after }}' AS starting_after,
    DATE_PART('EPOCH', SYSDATE()) :: INTEGER AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/api/loyalty/transaction_entries' || '?organizationId=' || '{organization_id}' || '&websiteId=' || '{website_id}' || '&limit=' || api_limit{% if not var('STOREFRONT_INITIAL_RUN', false) %} || '&startingAfter=' || '{{ starting_after }}'{% endif %} || '&sortDir=asc',
        { 'x-api-key': '{Authentication}' },
        {},
        'Vault/prod/flow/snag-api'
    ) AS request
