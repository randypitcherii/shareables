{#-
  Resolve a human-readable, SQL-safe identifier for the current developer, used to
  isolate development schemas (randy.pitcher -> randy_pitcher, giving dbt_randy_pitcher).

  IMPORTANT: dbt resolves schema names at PARSE time, when there is no warehouse
  connection (execute == false), so we CANNOT run `select current_user()` here. We
  resolve from the environment instead -- which also keeps onboarding zero-config:

    1. DBT_DEV_USER env var, if set   (explicit override)
    2. the OS `USER` env var           (automatic on a developer's machine)
    3. the static fallback 'dev_user'

  Set DBT_DEV_USER if your OS username doesn't match the identity you want in the
  schema name (e.g. on a shared CI box, though CI uses ci_testing and never calls this).
-#}
{% macro dbt_clean_username() -%}

  {%- set raw = env_var('DBT_DEV_USER', env_var('USER', 'dev_user')) -%}
  {%- if raw == '' -%}
    {%- set raw = 'dev_user' -%}
  {%- endif -%}

  {#- drop any email domain, lower-case, collapse non-alphanumerics to "_" -#}
  {%- set local_part = raw.split('@')[0] | lower -%}
  {%- set cleaned = modules.re.sub('[^a-z0-9]+', '_', local_part) -%}
  {{ cleaned | trim('_') }}

{%- endmacro %}
