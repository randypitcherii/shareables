{#-
  Helpers for the S3 Inventory storage pipeline.

  s3_inventory_use_sample_data()
      True when no real inventory table is configured (DBT_S3_INVENTORY_TABLE unset).
      The staging models then read the committed sample seeds instead of real
      sources -- the same zero-config posture as the rest of the project, and what
      CI builds and tests against. Resolved at PARSE time from an env var, so the
      unused branch's ref()/source() never enters the DAG.

  s3_inventory_normalize_uri(expr)
      Canonical form for comparing S3 locations: s3a:// / s3n:// / S3:// -> s3://,
      trailing slashes trimmed. Applied to BOTH sides of every path comparison.

  s3_inventory_bytes_to_gib(expr)
      AWS bills storage per GB-month where GB = 2^30 bytes.
-#}

{% macro s3_inventory_use_sample_data() -%}
  {{- return(var('s3_inventory_table', '') | trim == '') -}}
{%- endmacro %}

{% macro s3_inventory_normalize_uri(expr) -%}
  regexp_replace(regexp_replace(trim({{ expr }}), '^(?i)s3[an]?://', 's3://'), '/+$', '')
{%- endmacro %}

{% macro s3_inventory_bytes_to_gib(expr) -%}
  (cast({{ expr }} as double) / 1073741824.0)
{%- endmacro %}

{#-
  s3_inventory_column(name, data_type, available_columns)

  Real inventories differ by configuration: a "current version only" report has no
  version_id / is_latest / is_delete_marker fields at all, and optional metadata
  fields (intelligent_tiering_access_tier, ...) exist only when selected. Emit the
  column when the relation has it, else a typed NULL, so one staging model fits
  every inventory configuration. `available_columns` is none at parse time (no
  warehouse connection) -- then assume the column exists.
-#}
{% macro s3_inventory_column(name, data_type, available_columns) -%}
  {%- if available_columns is none or name | lower in available_columns -%}
    cast({{ name }} as {{ data_type }})
  {%- else -%}
    cast(null as {{ data_type }})
  {%- endif -%}
{%- endmacro %}
