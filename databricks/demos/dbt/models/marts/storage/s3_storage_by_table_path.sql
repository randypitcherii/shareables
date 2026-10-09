{#-
  Storage per table root -- UC-registered tables AND unregistered Delta tables found
  by their `_delta_log/` -- plus one '(untracked)' row per bucket for everything no
  table owns. Grain: one row per bucket + table_path.

  noncurrent_bytes is the hidden cost of Delta on a versioned bucket: VACUUM and
  OPTIMIZE "delete" files, but versioning keeps every one as a noncurrent version
  until a lifecycle rule expires it.
-#}
with objects as (
    select * from {{ ref('int_s3_inventory__objects_classified') }}
),

table_paths as (
    select * from {{ ref('int_s3_inventory__table_paths') }}
),

rollup as (

    select
        bucket,
        coalesce(table_uri, '(untracked)') as table_path,
        count_if(version_state = 'current')                                       as current_object_count,
        coalesce(sum(size_bytes) filter (where version_state = 'current'), 0)     as current_bytes,
        coalesce(sum(size_bytes) filter (where version_state = 'current' and object_kind = 'delta_log'), 0) as delta_log_bytes,
        count_if(version_state = 'noncurrent')                                    as noncurrent_version_count,
        coalesce(sum(size_bytes) filter (where version_state = 'noncurrent'), 0)  as noncurrent_bytes,
        count_if(version_state = 'delete_marker')                                 as delete_marker_count,
        coalesce(sum(size_bytes), 0)                                              as total_bytes,
        min(last_modified_at) filter (where version_state = 'current')           as oldest_object_at,
        max(last_modified_at) filter (where version_state = 'current')           as newest_object_at,
        sum(monthly_cost_usd)                                                     as monthly_cost_usd,
        coalesce(sum(monthly_cost_usd) filter (where version_state = 'noncurrent'), 0) as noncurrent_monthly_cost_usd,
        coalesce(sum(estimated_monthly_savings_usd), 0)                           as estimated_monthly_savings_usd
    from objects
    group by bucket, coalesce(table_uri, '(untracked)')

)

select
    rollup.bucket,
    rollup.table_path,
    coalesce(table_paths.path_source, 'untracked')   as path_source,
    table_paths.table_full_name,
    table_paths.registered_table_count,
    table_paths.table_type,
    table_paths.data_source_format,
    coalesce(table_paths.activity_status, 'n/a')     as activity_status,
    table_paths.last_read_at,
    table_paths.last_write_at,
    table_paths.days_since_last_access,

    rollup.current_object_count,
    rollup.current_bytes,
    rollup.delta_log_bytes,
    rollup.noncurrent_version_count,
    rollup.noncurrent_bytes,
    rollup.delete_marker_count,
    rollup.total_bytes,
    round(rollup.total_bytes / power(1024, 3), 6) as total_gib,
    rollup.oldest_object_at,
    rollup.newest_object_at,
    rollup.monthly_cost_usd,
    rollup.noncurrent_monthly_cost_usd,
    rollup.estimated_monthly_savings_usd

from rollup
left join table_paths
    on table_paths.table_uri = rollup.table_path
