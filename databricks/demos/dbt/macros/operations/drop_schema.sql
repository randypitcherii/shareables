{#-
  Drop a schema (and everything in it) in the target catalog. Intended for tearing
  down ephemeral CI schemas as a run-operation:

      dbt run-operation drop_schema --args '{schema: dbt_proj_pr42_build3}' --target ci

  catalog defaults to the `default_catalog` var. Destructive by design -- only ever
  invoke it explicitly (e.g. the teardown step of a PR CI job).
-#}
{% macro drop_schema(schema, catalog=none) %}
  {%- set catalog = catalog or var('default_catalog', target.database) -%}
  {%- if execute -%}
    {%- set fqn = adapter.quote(catalog) ~ '.' ~ adapter.quote(schema) -%}
    {{ log('Dropping schema ' ~ fqn ~ ' (cascade)', info=True) }}
    {%- do run_query('drop schema if exists ' ~ fqn ~ ' cascade') -%}
  {%- endif -%}
{% endmacro %}
