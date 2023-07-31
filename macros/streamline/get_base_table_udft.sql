{% macro create_udtf_get_base_table(schema) %}
create or replace function {{ schema }}.udtf_get_base_table(max_height integer)
returns table (height number, node_url varchar(16777216))
as
$$
    with base as (
        select
            row_number() over (
                order by
                    seq4()
            ) as id
        from
            table(generator(rowcount => 100000000)) -- July 2023 Flow Chain head is at  57M
    ),
    node_mapping as (
        select
            base.id as height,
            first_value(nv.node_url) over (partition by base.id order by nv.root_height desc) as node_url
        from
            base
        left join flow_dev.seeds.network_version nv
        on
            base.id >= nv.root_height
            and
            base.id <= max_height
    )
select
    height,
    coalesce(node_url, 'access.mainnet.nodes.onflow.org:9000') as node_url
from
    node_mapping
where
    height <= max_height
$$
;
{% endmacro %}