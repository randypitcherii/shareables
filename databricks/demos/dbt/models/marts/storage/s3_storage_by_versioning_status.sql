{#-
  Buckets grouped by inferred versioning status (enabled / suspended / disabled /
  unknown). The headline question it answers: how much are we paying to keep
  noncurrent versions, and on which buckets? Grain: one row per versioning_status.
-#}
select
    versioning_status,
    count(*)                                   as bucket_count,
    array_sort(collect_list(bucket))           as buckets,
    sum(current_bytes)                         as current_bytes,
    sum(noncurrent_bytes)                      as noncurrent_bytes,
    sum(total_bytes)                           as total_bytes,
    try_divide(sum(noncurrent_bytes), sum(total_bytes)) as noncurrent_share,
    sum(delete_marker_count)                   as delete_marker_count,
    sum(monthly_cost_usd)                      as monthly_cost_usd,
    sum(noncurrent_monthly_cost_usd)           as noncurrent_monthly_cost_usd,
    sum(estimated_monthly_savings_usd)         as estimated_monthly_savings_usd
from {{ ref('s3_storage_by_bucket') }}
group by versioning_status
