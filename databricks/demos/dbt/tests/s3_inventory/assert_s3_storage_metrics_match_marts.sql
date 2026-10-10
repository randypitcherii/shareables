{#-
  The metric view must tell the same story as the marts (runs in every mode). It
  joins bucket_summary with at_most_one_match asserted; if that join ever fanned
  out, or a measure expression drifted from the mart logic, these totals split.
  Compared per bucket, so a mismatch names the bucket.
-#}
with metric_view as (
    select
        `Bucket`                              as bucket,
        `Versioning Status`                   as versioning_status,
        MEASURE(`Total Storage`)              as total_bytes,
        MEASURE(`Noncurrent Storage`)         as noncurrent_bytes,
        MEASURE(`Monthly Storage Cost`)       as monthly_cost_usd,
        MEASURE(`Estimated Monthly Savings`)  as savings_usd
    from {{ ref('s3_storage_metrics') }}
    group by all
),

marts as (
    select bucket, versioning_status, total_bytes, noncurrent_bytes, monthly_cost_usd,
           estimated_monthly_savings_usd as savings_usd
    from {{ ref('s3_storage_by_bucket') }}
)

select
    coalesce(marts.bucket, metric_view.bucket) as bucket,
    marts.total_bytes        as mart_total_bytes,
    metric_view.total_bytes  as metric_view_total_bytes,
    marts.savings_usd        as mart_savings_usd,
    metric_view.savings_usd  as metric_view_savings_usd
from marts
full outer join metric_view
    on metric_view.bucket = marts.bucket
where marts.bucket is null
   or metric_view.bucket is null
   or metric_view.versioning_status != marts.versioning_status
   or metric_view.total_bytes != marts.total_bytes
   or coalesce(metric_view.noncurrent_bytes, 0) != marts.noncurrent_bytes
   or abs(metric_view.monthly_cost_usd - marts.monthly_cost_usd) > 0.000001
   or abs(metric_view.savings_usd - marts.savings_usd) > 0.000001
