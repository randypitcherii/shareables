{#-
  Storage split by who owns it and whether anyone still uses it. Grain: one row per
  bucket + path_source + activity_status.

    path_source      unity_catalog | inferred_delta_log | untracked
    activity_status  active | inactive (read/written within s3_inventory_active_days
                     of the snapshot, per table lineage) | n/a (untracked data)

  The money row is usually (unity_catalog, inactive): tables still registered, still
  billed, and not touched in months.
-#}
select
    bucket,
    path_source,
    activity_status,
    count(distinct case when path_source != 'untracked' then table_path end) as table_count,
    sum(current_object_count)          as current_object_count,
    sum(current_bytes)                 as current_bytes,
    sum(noncurrent_bytes)              as noncurrent_bytes,
    sum(total_bytes)                   as total_bytes,
    sum(monthly_cost_usd)              as monthly_cost_usd,
    sum(estimated_monthly_savings_usd) as estimated_monthly_savings_usd
from {{ ref('s3_storage_by_table_path') }}
group by bucket, path_source, activity_status
