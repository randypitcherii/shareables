{#-
  Environment-aware schema routing.

  Reads the `deployment_environment` project var (sourced from the
  DBT_DEPLOYMENT_ENVIRONMENT env var, default 'development').
  Accepted values: development | ci_testing | production.

    development  -> EVERY model lands in ONE isolated, per-user schema:
                    "{default_schema}_{clean_username}"  (e.g. dbt_randy_pitcher).
                    Per-layer +schema configs are intentionally IGNORED so a
                    developer's whole build stays in their personal sandbox and
                    never collides with a teammate's.

    ci_testing   -> the default_schema is used UNMODIFIED. CI injects a
                    build-scoped name via DBT_DEFAULT_SCHEMA
                    (e.g. dbt_<project>_pr<pr_number>_build<build_number>), giving
                    every PR build a fully isolated, disposable namespace.

    production   -> the default_schema is used ONLY when a model declares no custom
                    schema; otherwise the model's custom (+schema) is used
                    UNMODIFIED -- so prod intentionally spreads across many
                    purpose-built namespaces (staging / modeled / mart / ...).
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}

  {%- set deployment_environment = var('deployment_environment') -%}
  {%- set default_schema = var('default_schema', target.schema) -%}

  {%- set valid_environments = ['development', 'ci_testing', 'production'] -%}
  {%- if deployment_environment not in valid_environments -%}
    {%- set msg -%}
Invalid deployment_environment: '{{ deployment_environment }}'
  Expected one of: {{ valid_environments }}
  Set it via the DBT_DEPLOYMENT_ENVIRONMENT environment variable
  (or the `deployment_environment` project var in dbt_project.yml).
    {%- endset -%}
    {{ exceptions.raise_compiler_error(msg) }}
  {%- endif -%}

  {%- if deployment_environment == 'development' -%}
    {{ (default_schema ~ '_' ~ dbt_clean_username()) | trim }}

  {%- elif deployment_environment == 'ci_testing' -%}
    {{ default_schema | trim }}

  {%- elif deployment_environment == 'production' -%}
    {%- if custom_schema_name is none -%}
      {{ default_schema | trim }}
    {%- else -%}
      {{ custom_schema_name | trim }}
    {%- endif -%}

  {%- endif -%}
{%- endmacro %}
