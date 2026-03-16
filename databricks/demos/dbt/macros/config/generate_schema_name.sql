{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set default_schema = target.schema -%}
  {%- set deployment_environment = var('deployment_environment') -%}

  {#- validate the current environment -#}
  {% if not deployment_environment in ('default', 'dev', 'ci', 'prod', 'databricks_cluster') %}
    {%- set msg -%}
      Error determining deployment environment:
        - expected one of 'default', 'dev', 'ci', or 'prod'
        - received {{deployment_environment}}
        - target:
          {{target}}

    {%- endset -%}
    {{ exceptions.raise_compiler_error(msg) }}
  {% endif %}

  {#- determine the schema gen pattern based on the valid deployment_environment -#}
  {%- if custom_schema_name is none or deployment_environment in ('default', 'dev', 'ci') -%}
    {#- if a custom schema isn't defined OR this is a dev/ci deployment, we're sticking to the default -#}
    {{ default_schema }}

  {%- elif deployment_environment in ('prod', 'databricks_cluster') -%}
    {# use custom_schema_name directly rather than concatenating #}
    {{ custom_schema_name | trim }}

  {%- endif -%}
{%- endmacro %}