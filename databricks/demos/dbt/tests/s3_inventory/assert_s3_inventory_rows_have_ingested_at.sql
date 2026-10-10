{#-
  House standard: every raw row records when it was ingested. Fails when any
  inventory row (any snapshot) has no ingested_at -- usually an ingestion that
  doesn't stamp it, or a raw table registered without the column (staging then
  emits NULL). Fix the ingestion (README: "Point it at real inventory"), don't
  relax the test: ingest latency is what the pipeline-health views monitor.
-#}
select
    bucket,
    inventory_snapshot,
    count(*) as rows_missing_ingested_at
from {{ ref('stg_s3_inventory__objects') }}
where ingested_at is null
group by bucket, inventory_snapshot
