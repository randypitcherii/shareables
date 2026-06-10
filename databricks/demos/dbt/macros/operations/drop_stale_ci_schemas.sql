{#-
  Drop CI schemas that outlived their PR. The per-PR teardown step in CI normally
  cleans up after itself, but cancelled runs, runner failures, and auth hiccups leak
  schemas -- this run-operation sweeps them up on a schedule (or ad hoc):

      # see what would be dropped (default is dry_run: true)
      dbt run-operation drop_stale_ci_schemas \
        --args '{prefix: dbt_rpw_dbt_databricks_reference_pr}' --target ci

      # actually drop
      dbt run-operation drop_stale_ci_schemas \
        --args '{prefix: dbt_rpw_dbt_databricks_reference_pr, older_than_days: 3, dry_run: false}' \
        --target ci

  `prefix` is required and must be non-empty -- it is what stops this from matching
  every schema in the catalog. Staleness comes from information_schema.schemata.created.
-#}
{% macro drop_stale_ci_schemas(prefix, older_than_days=3, catalog=none, dry_run=true) %}
  {%- if not prefix -%}
    {{ exceptions.raise_compiler_error('drop_stale_ci_schemas requires a non-empty `prefix` -- refusing to match every schema in the catalog.') }}
  {%- endif -%}
  {%- set catalog = catalog or var('default_catalog', target.database) -%}
  {%- if execute -%}
    {%- set find_stale -%}
      select schema_name
      from {{ adapter.quote(catalog) }}.information_schema.schemata
      where schema_name like '{{ prefix }}%'
        and created < current_timestamp() - interval {{ older_than_days }} days
      order by schema_name
    {%- endset -%}
    {%- set stale = run_query(find_stale) -%}
    {%- if stale.rows | length == 0 -%}
      {{ log('No schemas in ' ~ catalog ~ ' match ' ~ prefix ~ '% older than ' ~ older_than_days ~ ' days.', info=True) }}
    {%- endif -%}
    {%- for row in stale.rows -%}
      {%- if dry_run -%}
        {{ log('[dry_run] would drop ' ~ catalog ~ '.' ~ row[0] ~ ' (pass dry_run: false to drop)', info=True) }}
      {%- else -%}
        {{ drop_schema(row[0], catalog) }}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}
