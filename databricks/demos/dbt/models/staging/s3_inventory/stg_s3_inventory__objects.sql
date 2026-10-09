{#-
  1:1 typed view over the S3 Inventory: every object version in every snapshot.

  Real mode reads the configured inventory source; sample mode (no
  DBT_S3_INVENTORY_TABLE) reads the committed sample seed. Optional inventory fields
  that the configured report doesn't include become typed NULLs (see
  s3_inventory_column), so current-version-only reports still build -- their
  buckets just classify as versioning 'unknown' downstream.
-#}
{%- set use_sample = s3_inventory_use_sample_data() -%}
{%- if use_sample -%}
  {%- set relation = ref('s3_inventory_sample__objects') -%}
{%- else -%}
  {%- set relation = source('s3_inventory', 'inventory') -%}
{%- endif -%}

{%- set available_columns = none -%}
{%- if execute and not use_sample -%}
  {%- set available_columns = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
{%- endif -%}

{%- set keys_url_encoded = var('s3_inventory_keys_url_encoded', 'false') | string | lower == 'true' -%}

with source as (
    select * from {{ relation }}
)

select
    -- identity
    cast(bucket as string) as bucket,
    {% if keys_url_encoded -%}
    -- CSV inventories URL-encode keys; Parquet / ORC deliver them raw
    url_decode(cast(key as string)) as key,
    {%- else -%}
    cast(key as string) as key,
    {%- endif %}

    -- versioning (absent from current-version-only reports -> NULL)
    {{ s3_inventory_column('version_id', 'string', available_columns) }}        as version_id,
    {{ s3_inventory_column('is_latest', 'boolean', available_columns) }}        as is_latest,
    {{ s3_inventory_column('is_delete_marker', 'boolean', available_columns) }} as is_delete_marker,

    -- size + age
    cast(size as bigint)                    as size_bytes,
    cast(last_modified_date as timestamp)   as last_modified_at,

    -- where it is billed
    upper(coalesce(cast(storage_class as string), 'STANDARD'))                              as storage_class,
    upper({{ s3_inventory_column('intelligent_tiering_access_tier', 'string', available_columns) }}) as intelligent_tiering_access_tier,
    {{ s3_inventory_column('is_multipart_uploaded', 'boolean', available_columns) }}        as is_multipart_uploaded,

    -- which inventory delivery this row came from. `dt` is 'YYYY-MM-DD-HH-MM', which
    -- sorts lexically, so max(dt) is the latest snapshot.
    {{ s3_inventory_column('dt', 'string', available_columns) }}                            as inventory_snapshot,
    to_date(substr({{ s3_inventory_column('dt', 'string', available_columns) }}, 1, 10))    as inventory_snapshot_date,

    '{{ "sample" if use_sample else "s3_inventory" }}' as inventory_source

from source
