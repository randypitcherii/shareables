{#-
  Route models to the catalog named by the `default_catalog` var (sourced from the
  DBT_DEFAULT_CATALOG env var, default analytics_dev), unless a
  model declares an explicit +catalog / +database, which is then honored as-is.

  This makes `default_catalog` the single, env-overridable source of truth for
  where models land -- the same pattern as default_schema.
-#}
{% macro generate_database_name(custom_database_name, node) -%}
  {%- set default_database = var('default_catalog', target.database) -%}
  {%- if custom_database_name is none -%}
    {{ default_database | trim }}
  {%- else -%}
    {{ custom_database_name | trim }}
  {%- endif -%}
{%- endmacro %}
