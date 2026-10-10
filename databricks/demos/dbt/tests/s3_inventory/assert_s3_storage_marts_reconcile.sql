{#-
  Reconciliation (runs in every mode): every byte in the latest snapshot must land in
  exactly one bucket row AND exactly one table_path row, and opportunity bytes can
  never exceed what exists. A fan-out in the prefix -> table match (an object
  claimed by two tables) or a dropped join shows up here as a mismatch, even when
  every mart builds green.
-#}
with objects as (
    select coalesce(sum(size_bytes), 0) as total_bytes from {{ ref('int_s3_inventory__objects') }}
),
buckets as (
    select coalesce(sum(total_bytes), 0) as total_bytes from {{ ref('s3_storage_by_bucket') }}
),
table_paths as (
    select coalesce(sum(total_bytes), 0) as total_bytes from {{ ref('s3_storage_by_table_path') }}
),
activity as (
    select coalesce(sum(total_bytes), 0) as total_bytes from {{ ref('s3_storage_by_table_activity') }}
),
opportunities as (
    select coalesce(sum(bytes), 0) as total_bytes from {{ ref('s3_storage_savings_opportunities') }}
)

select
    objects.total_bytes       as object_bytes,
    buckets.total_bytes       as bucket_bytes,
    table_paths.total_bytes   as table_path_bytes,
    activity.total_bytes      as activity_bytes,
    opportunities.total_bytes as opportunity_bytes
from objects, buckets, table_paths, activity, opportunities
where buckets.total_bytes     != objects.total_bytes
   or table_paths.total_bytes != objects.total_bytes
   or activity.total_bytes    != objects.total_bytes
   or opportunities.total_bytes > objects.total_bytes
