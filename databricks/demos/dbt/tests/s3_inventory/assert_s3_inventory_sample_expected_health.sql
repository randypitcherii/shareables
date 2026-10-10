{{ config(enabled = s3_inventory_use_sample_data()) }}
{#-
  Correctness on the committed sample (sample mode only): each health check fires
  on exactly the snapshot built to trip it, and every other snapshot is healthy.
    * acme-scratch-current-only ingested ~32 h after its snapshot -> late_ingest
    * acme-archive's latest snapshot came 5 days after the previous -> delivery_gap
      (its bytes also doubled; the gap is reported first)
    * acme-lakehouse-prod's 01-14 snapshot holds one 1000 TiB "ghost" row -> its
      bytes swing on both sides of it -> volume_shift twice
-#}
with expected (bucket, inventory_snapshot, row_count, ingest_lag_hours, health_status) as (
    values
        ('acme-lakehouse-prod',       '2026-01-13-01-00',  2,  5.5, 'healthy'),
        ('acme-lakehouse-prod',       '2026-01-14-01-00',  1,  5.5, 'volume_shift'),
        ('acme-lakehouse-prod',       '2026-01-15-01-00', 14,  5.5, 'volume_shift'),
        ('acme-logs',                 '2026-01-13-01-00',  2,  5.5, 'healthy'),
        ('acme-logs',                 '2026-01-14-01-00',  2,  5.5, 'healthy'),
        ('acme-logs',                 '2026-01-15-01-00',  4,  5.5, 'healthy'),
        ('acme-archive',              '2026-01-10-01-00',  2,  5.5, 'healthy'),
        ('acme-archive',              '2026-01-15-01-00',  3,  5.5, 'delivery_gap'),
        ('acme-scratch-current-only', '2026-01-15-01-00',  2, 32.0, 'late_ingest')
),

actual as (
    select bucket, inventory_snapshot, row_count, cast(ingest_lag_hours as decimal(10, 1)) as ingest_lag_hours, health_status
    from {{ ref('s3_inventory_snapshot_health') }}
)

(select 'missing or wrong' as problem, bucket, inventory_snapshot, row_count, cast(ingest_lag_hours as decimal(10, 1)), health_status from expected
 except
 select 'missing or wrong', * from actual)
union all
(select 'unexpected' as problem, * from actual
 except
 select 'unexpected', bucket, inventory_snapshot, row_count, cast(ingest_lag_hours as decimal(10, 1)), health_status from expected)
