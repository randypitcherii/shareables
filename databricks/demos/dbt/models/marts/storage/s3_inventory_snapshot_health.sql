{#-
  Pipeline health of the S3 Inventory feed: one row per bucket per inventory
  snapshot, across ALL snapshots in the raw table (the storage marts only read the
  latest). It answers "can I trust today's numbers?":

    * did each daily delivery arrive, and on time?    (snapshot gaps, ingest lag)
    * is every row stamped with ingested_at?           (house standard on raw data)
    * did the inventory suddenly shrink or balloon?    (bytes vs previous snapshot)
    * is every storage class priced?                   (unpriced rows zero out cost)

  health_status picks the FIRST failing check, in that order of severity. Whether
  the feed is stale right now depends on the clock, so that check lives in the
  metric view (s3_inventory_pipeline_health_metrics), not here. `built_at` records
  when dbt last rebuilt this table -- the dbt side of the pipeline.

  Cost note: this scans every retained snapshot each build. Keep the raw table's
  snapshot retention bounded (the README suggests ~30-90 days).
-#}
{%- set max_lag_hours = var('s3_inventory_max_ingest_lag_hours', 24) | float -%}
{%- set max_gap_hours = var('s3_inventory_max_snapshot_gap_hours', 36) | float -%}
{%- set max_volume_change = var('s3_inventory_max_volume_change', 0.5) | float -%}

with objects as (
    select * from {{ ref('stg_s3_inventory__objects') }}
),

pricing as (
    select pricing_tier from {{ ref('s3_storage_class_pricing') }}
),

priced as (
    select
        objects.*,
        pricing.pricing_tier is not null or coalesce(objects.is_delete_marker, false) as is_priced
    from objects
    left join pricing
        on pricing.pricing_tier = case
            when objects.storage_class = 'INTELLIGENT_TIERING'
            then concat('INTELLIGENT_TIERING:', coalesce(objects.intelligent_tiering_access_tier, 'FREQUENT'))
            else objects.storage_class
        end
),

snapshots as (
    select
        bucket,
        inventory_snapshot,
        max(inventory_snapshot_at)  as inventory_snapshot_at,
        max(inventory_source)       as inventory_source,
        count(*)                    as row_count,
        count_if(coalesce(is_latest, true) and not coalesce(is_delete_marker, false)) as current_object_count,
        coalesce(sum(size_bytes), 0) as total_bytes,
        min(ingested_at)            as first_ingested_at,
        max(ingested_at)            as last_ingested_at,
        count_if(ingested_at is null) as rows_missing_ingested_at,
        count_if(not is_priced)     as unpriced_rows,
        count_if(size_bytes is null or key is null) as rows_missing_size_or_key
    from priced
    group by bucket, inventory_snapshot
),

with_previous as (
    select
        snapshots.*,
        lag(inventory_snapshot_at) over (partition by bucket order by inventory_snapshot) as previous_snapshot_at,
        lag(total_bytes)           over (partition by bucket order by inventory_snapshot) as previous_total_bytes,
        lag(row_count)             over (partition by bucket order by inventory_snapshot) as previous_row_count,
        inventory_snapshot = max(inventory_snapshot) over (partition by bucket)          as is_latest_snapshot
    from snapshots
),

measured as (
    select
        with_previous.*,
        timestampdiff(MINUTE, inventory_snapshot_at, last_ingested_at) / 60.0   as ingest_lag_hours,
        timestampdiff(MINUTE, previous_snapshot_at, inventory_snapshot_at) / 60.0 as hours_since_previous_snapshot,
        try_divide(total_bytes - previous_total_bytes, previous_total_bytes)     as bytes_change_pct,
        try_divide(row_count - previous_row_count, previous_row_count)           as row_count_change_pct
    from with_previous
)

select
    bucket,
    inventory_snapshot,
    inventory_snapshot_at,
    cast(inventory_snapshot_at as date) as inventory_snapshot_date,
    inventory_source,
    is_latest_snapshot,
    row_count,
    current_object_count,
    total_bytes,
    first_ingested_at,
    last_ingested_at,
    rows_missing_ingested_at,
    unpriced_rows,
    rows_missing_size_or_key,
    ingest_lag_hours,
    previous_snapshot_at,
    hours_since_previous_snapshot,
    previous_total_bytes,
    bytes_change_pct,
    previous_row_count,
    row_count_change_pct,
    case
        when rows_missing_ingested_at > 0                          then 'missing_ingested_at'
        when rows_missing_size_or_key > 0                          then 'malformed_rows'
        when ingest_lag_hours > {{ max_lag_hours }}                then 'late_ingest'
        when hours_since_previous_snapshot > {{ max_gap_hours }}   then 'delivery_gap'
        when abs(bytes_change_pct) > {{ max_volume_change }}       then 'volume_shift'
        when unpriced_rows > 0                                     then 'unpriced_storage'
        else                                                            'healthy'
    end as health_status,
    current_timestamp() as built_at

from measured
