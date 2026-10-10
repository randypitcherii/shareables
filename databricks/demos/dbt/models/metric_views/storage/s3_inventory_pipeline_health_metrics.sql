{{ config(materialized='metric_view') }}
{%- set max_gap_hours = var('s3_inventory_max_snapshot_gap_hours', 36) | float -%}
# Health of the S3 Inventory pipeline: is the feed arriving, on time, complete,
# and is dbt rebuilding it? Backs the "Pipeline health" tab of the S3 Storage
# dashboard. Thresholds come from the s3_inventory_max_* project vars.

version: 1.1

# Grain: one bucket x inventory snapshot, across every snapshot retained in the
# raw inventory table (see s3_inventory_snapshot_health).
source: {{ ref('s3_inventory_snapshot_health') }}

comment: >-
  Health of the S3 Inventory data pipeline, one row per bucket per inventory
  snapshot. Use it before trusting s3_storage_metrics. Healthy means: every
  bucket's latest snapshot is under {{ max_gap_hours | int }} hours old, it was
  ingested promptly, no delivery was skipped, the volume didn't swing
  unexpectedly, every row carries ingested_at, and every storage class is priced.
  Health Status names the first failing check per snapshot; the Stale Buckets and
  Hours Since measures are evaluated against the clock at query time. Buckets
  with inventory_source = sample are the committed demo data and always look stale.

dimensions:
  - name: Bucket
    expr: source.bucket
    display_name: Bucket
    comment: S3 bucket the inventory snapshot describes.
    synonyms: [s3 bucket, bucket name]
  - name: Snapshot
    expr: source.inventory_snapshot
    display_name: Snapshot
    comment: The inventory delivery id (the `dt` partition, YYYY-MM-DD-HH-MM).
    synonyms: [dt, delivery, inventory run]
  - name: Snapshot Time
    expr: source.inventory_snapshot_at
    display_name: Snapshot Time
    comment: When S3 took the inventory snapshot (UTC).
    synonyms: [snapshot at, inventory time, report time]
    format:
      type: date_time
      date_format: year_month_day
      time_format: locale_hour_minute
  - name: Snapshot Date
    expr: source.inventory_snapshot_date
    display_name: Snapshot Date
    comment: Date of the inventory snapshot; use for daily trends.
    synonyms: [inventory date, day]
    format:
      type: date
      date_format: year_month_day
  - name: Is Latest Snapshot
    expr: source.is_latest_snapshot
    display_name: Is Latest Snapshot
    comment: True for each bucket's newest snapshot -- the one the storage numbers are built from. Filter to true for "right now" health.
    synonyms: [latest, current snapshot, newest]
  - name: Health Status
    expr: source.health_status
    display_name: Health Status
    comment: >-
      First failing check for the snapshot: missing_ingested_at (rows without the
      ingest timestamp), malformed_rows (null key or size), late_ingest (landed more
      than s3_inventory_max_ingest_lag_hours after the snapshot), delivery_gap (more
      than s3_inventory_max_snapshot_gap_hours since the previous snapshot),
      volume_shift (bytes moved more than s3_inventory_max_volume_change vs the
      previous snapshot), unpriced_storage (storage class missing from the pricing
      seed) -- or healthy.
    synonyms: [status, health, check, problem, issue]
  - name: Is Healthy
    expr: source.health_status = 'healthy'
    display_name: Is Healthy
    comment: True when every check passed for the snapshot.
    synonyms: [healthy, ok, passing]
  - name: Inventory Source
    expr: source.inventory_source
    display_name: Inventory Source
    comment: s3_inventory for real data; sample for the committed demo data.
    synonyms: [data source, sample data]

measures:
  # ---- freshness (evaluated at query time) ---------------------------------------
  - name: Latest Snapshot Time
    expr: MAX(source.inventory_snapshot_at)
    display_name: Latest Snapshot Time
    comment: Newest inventory snapshot in the slice.
    synonyms: [last snapshot, most recent inventory]
    format:
      type: date_time
      date_format: year_month_day
      time_format: locale_hour_minute
  - name: Hours Since Latest Snapshot
    expr: timestampdiff(MINUTE, MAX(source.inventory_snapshot_at), current_timestamp()) / 60.0
    display_name: Hours Since Latest Snapshot
    comment: Age of the newest snapshot right now. S3 Inventory delivers daily, so above ~{{ max_gap_hours | int }} means a delivery is missing.
    synonyms: [staleness, snapshot age, how stale, freshness]
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: Stale Buckets
    expr: COUNT(DISTINCT source.bucket) FILTER (WHERE source.is_latest_snapshot AND source.inventory_snapshot_at < current_timestamp() - INTERVAL {{ max_gap_hours | int }} HOURS)
    display_name: Stale Buckets
    comment: Buckets whose latest snapshot is older than s3_inventory_max_snapshot_gap_hours right now. Should be 0.
    synonyms: [stale, buckets behind, missing deliveries]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Last Ingested At
    expr: MAX(source.last_ingested_at)
    display_name: Last Ingested At
    comment: When the newest inventory row landed in the lakehouse (max ingested_at).
    synonyms: [last ingest, last load, ingested at]
    format:
      type: date_time
      date_format: year_month_day
      time_format: locale_hour_minute
  - name: Hours Since Last Ingest
    expr: timestampdiff(MINUTE, MAX(source.last_ingested_at), current_timestamp()) / 60.0
    display_name: Hours Since Last Ingest
    comment: How long since anything new was ingested. Rising while Hours Since Latest Snapshot is fine means the dbt rebuild, not ingestion, is behind (check Hours Since dbt Build).
    synonyms: [ingest staleness, load age]
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: Last dbt Build
    expr: MAX(source.built_at)
    display_name: Last dbt Build
    comment: When dbt last rebuilt the health table -- i.e. when the dbt side of the pipeline last ran.
    synonyms: [last build, last run, dbt run, built at]
    format:
      type: date_time
      date_format: year_month_day
      time_format: locale_hour_minute
  - name: Hours Since dbt Build
    expr: timestampdiff(MINUTE, MAX(source.built_at), current_timestamp()) / 60.0
    display_name: Hours Since dbt Build
    comment: Age of the last dbt build. The daily job should keep this under ~24.
    synonyms: [build age, dbt staleness]
    format:
      type: number
      decimal_places:
        type: max
        places: 1

  # ---- delivery + latency ---------------------------------------------------------
  - name: Snapshots
    expr: COUNT(1)
    display_name: Snapshots
    comment: Bucket-snapshots in the slice.
    synonyms: [deliveries, snapshot count, inventory runs]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Unhealthy Snapshots
    expr: COUNT_IF(source.health_status != 'healthy')
    display_name: Unhealthy Snapshots
    comment: Bucket-snapshots with at least one failing check.
    synonyms: [failures, failing snapshots, problems, issues]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Healthy Share
    expr: try_divide(COUNT_IF(source.health_status = 'healthy'), COUNT(1))
    display_name: Healthy Share
    comment: Share of bucket-snapshots passing every check.
    synonyms: [health rate, pass rate, success rate]
    format:
      type: percentage
      decimal_places:
        type: max
        places: 0
  - name: Buckets Reporting
    expr: COUNT(DISTINCT source.bucket)
    display_name: Buckets Reporting
    comment: Distinct buckets with at least one snapshot in the slice.
    synonyms: [buckets, bucket count]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Avg Ingest Lag Hours
    expr: AVG(source.ingest_lag_hours)
    display_name: Avg Ingest Lag (hours)
    comment: Mean hours from snapshot to ingested.
    synonyms: [ingest latency, average lag, load delay]
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: Max Ingest Lag Hours
    expr: MAX(source.ingest_lag_hours)
    display_name: Max Ingest Lag (hours)
    comment: Worst hours from snapshot to ingested in the slice.
    synonyms: [worst lag, max latency]
    format:
      type: number
      decimal_places:
        type: max
        places: 1
  - name: Max Snapshot Gap Hours
    expr: MAX(source.hours_since_previous_snapshot)
    display_name: Max Gap Between Snapshots (hours)
    comment: Longest gap between consecutive snapshots of a bucket. ~24 for a healthy daily inventory.
    synonyms: [delivery gap, missed delivery, gap]
    format:
      type: number
      decimal_places:
        type: max
        places: 1

  # ---- completeness ---------------------------------------------------------------
  - name: Rows Ingested
    expr: SUM(source.row_count)
    display_name: Rows Ingested
    comment: Inventory rows (object versions) across the snapshots in the slice. Slice by Snapshot Date for a per-day volume.
    synonyms: [rows, records, row count, volume]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Inventoried Storage
    expr: SUM(source.total_bytes)
    display_name: Inventoried Storage
    comment: Bytes listed by the snapshots in the slice. Summing across days double counts -- slice by Snapshot Date (trend) or filter Is Latest Snapshot.
    synonyms: [bytes, size, storage, volume]
    format:
      type: byte
      decimal_places:
        type: max
        places: 1
  - name: Rows Missing ingested_at
    expr: SUM(source.rows_missing_ingested_at)
    display_name: Rows Missing ingested_at
    comment: Rows without an ingest timestamp. Must be 0 -- the build fails otherwise.
    synonyms: [missing ingested_at, unstamped rows]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Unpriced Rows
    expr: SUM(source.unpriced_rows)
    display_name: Unpriced Rows
    comment: Rows whose storage class is missing from the pricing seed (their cost is NULL). Add the class to s3_storage_class_pricing.csv.
    synonyms: [unpriced, missing price]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Max Volume Change
    expr: MAX(abs(source.bytes_change_pct))
    display_name: Max Volume Change
    comment: Largest absolute change in bytes vs the previous snapshot.
    synonyms: [volume swing, size change, anomaly]
    format:
      type: percentage
      decimal_places:
        type: max
        places: 0
