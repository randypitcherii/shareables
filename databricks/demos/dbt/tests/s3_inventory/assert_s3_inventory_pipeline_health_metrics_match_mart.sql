{#-
  The health metric view must agree with its mart (runs in every mode): same
  snapshot count, unhealthy count and rows per bucket.
-#}
with metric_view as (
    select
        `Bucket`                         as bucket,
        MEASURE(`Snapshots`)             as snapshots,
        MEASURE(`Unhealthy Snapshots`)   as unhealthy,
        MEASURE(`Rows Ingested`)         as row_count
    from {{ ref('s3_inventory_pipeline_health_metrics') }}
    group by all
),

mart as (
    select bucket, count(*) as snapshots, count_if(health_status != 'healthy') as unhealthy, sum(row_count) as row_count
    from {{ ref('s3_inventory_snapshot_health') }}
    group by bucket
)

select mart.*, metric_view.snapshots as mv_snapshots, metric_view.unhealthy as mv_unhealthy, metric_view.row_count as mv_rows
from mart
full outer join metric_view
    on metric_view.bucket = mart.bucket
where mart.bucket is null
   or metric_view.bucket is null
   or metric_view.snapshots != mart.snapshots
   or metric_view.unhealthy != mart.unhealthy
   or metric_view.row_count != mart.row_count
