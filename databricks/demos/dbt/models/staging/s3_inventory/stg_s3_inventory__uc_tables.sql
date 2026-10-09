{#-
  Unity Catalog tables that live on S3, with a normalized storage location -- the
  lookup that maps inventory prefixes back to the table that owns them.

  Real mode reads system.information_schema.tables (every table visible to the
  running principal; tables it cannot see show up downstream as UNREGISTERED data,
  so run production as a principal with metastore-wide visibility). Sample mode
  reads the committed sample seed.
-#}
{%- if s3_inventory_use_sample_data() -%}
  {%- set relation = ref('s3_inventory_sample__tables') -%}
{%- else -%}
  {%- set relation = source('system_information_schema', 'tables') -%}
{%- endif -%}

with source as (
    select * from {{ relation }}
)

select
    concat_ws('.', table_catalog, table_schema, table_name) as table_full_name,
    table_catalog,
    table_schema,
    table_name,
    table_type,
    data_source_format,
    storage_path,
    {{ s3_inventory_normalize_uri('storage_path') }}                               as table_uri,
    regexp_extract({{ s3_inventory_normalize_uri('storage_path') }}, '^s3://([^/]+)', 1) as bucket,
    created      as created_at,
    last_altered as last_altered_at

from source
where storage_path is not null
  and lower(storage_path) rlike '^s3[an]?://'
