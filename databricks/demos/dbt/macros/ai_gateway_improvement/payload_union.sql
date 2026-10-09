{#-
  Build one UNION ALL over every inference (payload) table this run can read.

  The system tables never store message content. Each Unity AI Gateway model
  service that has inference tables enabled logs request/response bodies to
  its OWN Delta table, and system.ai_gateway.usage names that table in
  endpoint_metadata.inference_table. So discovery is data-driven:

    1. distinct inference_table names seen in the usage mirror
    2. keep the tables this principal can actually SELECT: information_schema
       lists tables you can merely BROWSE, so filter on table_privileges
       (which includes privileges inherited from the catalog or schema) and
       table ownership, matching the user or any account group they are in.
       No access -> silently skipped, never a failed build.
    3. one SELECT per table, projecting a fixed column set; columns an older
       table version lacks become NULL

  Runs at EXECUTE time (the list changes as services enable logging), so the
  view is re-created on every build. With no readable table it compiles to
  an empty, correctly-typed relation and everything downstream stays green.
-#}
{% macro ai_gateway_payload_union(usage_relation) %}

{%- set wanted = {
    'request_id': 'string',
    'invocation_id': 'string',
    'event_time': 'timestamp',
    'status_code': 'int',
    'api_type': 'string',
    'request': 'string',
    'response': 'string',
    'logging_error_codes': 'array<string>',
} -%}

{%- set tables = [] -%}
{%- if execute -%}
  {%- set discover -%}
    with seen as (
        select distinct inference_table as fqn
        from {{ usage_relation }}
        where inference_table is not null and inference_table != ''
    ),
    parts as (
        select fqn,
               split(fqn, '\\.')[0] as c, split(fqn, '\\.')[1] as s, split(fqn, '\\.')[2] as t
        from seen
        where size(split(fqn, '\\.')) = 3
    ),
    readable as (
        select distinct p.c, p.s, p.t
        from parts p
        join system.information_schema.tables tbl
          on tbl.table_catalog = p.c and tbl.table_schema = p.s and tbl.table_name = p.t
        left join system.information_schema.table_privileges priv
          on priv.table_catalog = p.c and priv.table_schema = p.s and priv.table_name = p.t
         and priv.privilege_type in ('SELECT', 'ALL_PRIVILEGES')
         and (priv.grantee = current_user() or is_account_group_member(priv.grantee))
        where priv.grantee is not null
           or tbl.table_owner = current_user()
           or is_account_group_member(tbl.table_owner)
    )
    select r.c, r.s, r.t, concat_ws(',', collect_set(lower(col.column_name))) as cols
    from readable r
    join system.information_schema.columns col
      on col.table_catalog = r.c and col.table_schema = r.s and col.table_name = r.t
    group by all
    order by 1, 2, 3
  {%- endset -%}
  {%- set result = run_query(discover) -%}
  {%- for row in result.rows -%}
    {%- do tables.append({'c': row[0], 's': row[1], 't': row[2], 'cols': (row[3] or '').split(',')}) -%}
  {%- endfor -%}
{%- endif -%}

{%- if tables | length == 0 %}
select
  {%- for name, dtype in wanted.items() %}
    cast(null as {{ dtype }}) as {{ name }},
  {%- endfor %}
    cast(null as string) as inference_table
where false
{%- else -%}
  {%- for tbl in tables %}
select
  {%- for name, dtype in wanted.items() %}
    {% if name in tbl.cols -%} cast({{ name }} as {{ dtype }}) {%- else -%} cast(null as {{ dtype }}) {%- endif %} as {{ name }},
  {%- endfor %}
    '{{ tbl.c }}.{{ tbl.s }}.{{ tbl.t }}' as inference_table
from `{{ tbl.c }}`.`{{ tbl.s }}`.`{{ tbl.t }}`
where event_time >= current_timestamp() - interval {{ var('ai_gateway_history_days') }} days
    {%- if not loop.last %}
union all
    {%- endif %}
  {%- endfor %}
{%- endif %}

{% endmacro %}
