{#
  ci_cleanup: end-of-run CI hygiene in ONE operation --
    1. drop THIS build's ephemeral schema
    2. sweep stale CI schemas (prefix-matched, older than `older_than_days`)
       leaked by earlier failed or cancelled runs

  House rule (THE_ONE_TRUE_WAY #13): every destructive run-operation takes
  `dry_run` as its FINAL argument, defaulting to TRUE, and a dry run prints the
  exact statements a live run would execute. This is a debugging superpower:
  you can always see precisely what a cleanup WOULD do before letting it.

      # see what would happen (the default)
      uv run dbt run-operation ci_cleanup \
        --args '{schema: dbt_rpw_dbt_databricks_reference_pr42_build3}' \
        --target ci --profiles-dir .

      # actually do it (what CI runs after a green build)
      uv run dbt run-operation ci_cleanup \
        --args '{schema: dbt_rpw_dbt_databricks_reference_pr42_build3, dry_run: False}' \
        --target ci --profiles-dir .

  `prefix` is required non-empty -- it is what stops the sweep from matching
  every schema in the catalog.
#}
{% macro ci_cleanup(schema, prefix='dbt_rpw_dbt_databricks_reference_pr', older_than_days=3, catalog=none, dry_run=true) %}
  {%- if not schema -%}
    {{ exceptions.raise_compiler_error('ci_cleanup requires the current build `schema`.') }}
  {%- endif -%}
  {%- if not prefix -%}
    {{ exceptions.raise_compiler_error('ci_cleanup requires a non-empty `prefix` -- refusing to sweep every schema in the catalog.') }}
  {%- endif -%}
  {%- set catalog = catalog or target.catalog -%}

  {%- set statements = [] -%}
  {%- do statements.append('drop schema if exists ' ~ adapter.quote(catalog) ~ '.' ~ adapter.quote(schema) ~ ' cascade') -%}

  {%- set stale = run_query(
        'select schema_name from ' ~ adapter.quote(catalog) ~ '.information_schema.schemata'
        ~ " where schema_name like '" ~ prefix ~ "%'"
        ~ " and schema_name != '" ~ schema ~ "'"
        ~ ' and created < current_timestamp() - interval ' ~ older_than_days ~ ' days'
      ) -%}
  {%- for row in stale.rows -%}
    {%- do statements.append('drop schema if exists ' ~ adapter.quote(catalog) ~ '.' ~ adapter.quote(row[0]) ~ ' cascade') -%}
  {%- endfor -%}

  {%- for statement in statements -%}
    {%- if dry_run -%}
      {{ log('DRY RUN (pass dry_run: False to execute): ' ~ statement, info=True) }}
    {%- else -%}
      {{ log('Executing: ' ~ statement, info=True) }}
      {%- do run_query(statement) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ log('ci_cleanup: ' ~ statements | length ~ ' statement(s), dry_run=' ~ dry_run, info=True) }}
{% endmacro %}
