{{ config(materialized='metric_view') }}
# S3 storage semantic layer over the S3 Inventory pipeline. Built for people AND
# agents: every field carries a display name, a comment that says how to use it,
# and synonyms for the words people actually type ("size", "spend", "waste").
# Backs the S3 Storage dashboard (dashboards/s3_storage.lvdash.json).

version: 1.1

# Grain: one S3 object VERSION (current version, noncurrent version, or delete
# marker) from each bucket's LATEST inventory snapshot, already attributed to the
# table that owns it and tagged with at most one savings opportunity.
source: {{ ref('int_s3_inventory__objects_classified') }}

comment: >-
  S3 storage from AWS S3 Inventory, one row per object version in each bucket's
  latest snapshot. Answers how big and how expensive each bucket, table and prefix
  is, how much of it is noncurrent versions, which tables nobody uses, and where
  storage can be saved. Costs are monthly S3 STORAGE list prices from the
  s3_storage_class_pricing seed (USD, not invoiced spend; no request, transfer or
  transition charges). Savings never double count: every byte carries at most one
  Savings Opportunity, so savings add up across any slice. Start with Total
  Storage, Monthly Storage Cost and Estimated Monthly Savings, sliced by Bucket, Table
  Path or Savings Opportunity. Check s3_inventory_pipeline_health_metrics before
  trusting a number: it shows whether the inventory feed is fresh and complete.

joins:
  # bucket-level facts that only make sense per bucket (versioning is inferred from
  # the whole bucket's version fields); exactly one row per bucket
  - name: bucket_summary
    source: {{ ref('s3_storage_by_bucket') }}
    "on": source.bucket = bucket_summary.bucket
    cardinality: many_to_one
    rely:
      at_most_one_match: true

dimensions:
  # ---- where ---------------------------------------------------------------
  - name: Bucket
    expr: source.bucket
    display_name: Bucket
    comment: S3 bucket name. Every number in this view is scoped to the bucket's latest inventory snapshot.
    synonyms: [s3 bucket, bucket name, storage bucket]
  - name: Versioning Status
    expr: bucket_summary.versioning_status
    display_name: Versioning Status
    comment: >-
      Bucket versioning, inferred from inventory version fields: enabled, suspended,
      disabled, or unknown (the inventory lists current versions only, so it cannot
      tell -- reconfigure it with "Include all versions").
    synonyms: [versioning, bucket versioning, is versioned, versioning enabled]
  - name: Is Table Bucket
    expr: source.is_table_bucket
    display_name: Hosts UC Tables
    comment: True when at least one Unity Catalog table stores data in the bucket. Only there is unregistered data a red flag.
    synonyms: [table bucket, lakehouse bucket, has tables]
  - name: Prefix
    expr: source.prefix
    display_name: Prefix (Folder)
    comment: The object's "directory" -- its key minus the file name. Empty string for objects at the bucket root. High cardinality.
    synonyms: [folder, directory, path, key prefix]
  - name: Top-Level Prefix
    expr: "coalesce(nullif(split_part(source.key, '/', 1), source.key), '(bucket root)')"
    display_name: Top-Level Prefix
    comment: First path segment of the key (e.g. "tables", "logs"), or "(bucket root)". The quickest way to see what a bucket is used for.
    synonyms: [top folder, root folder, first prefix, area]
  - name: Object Key
    expr: source.key
    display_name: Object Key
    comment: Full S3 object key. Very high cardinality -- filter to a bucket and table first, use for drill-down only.
    synonyms: [key, file, file name, object]
  - name: Object URI
    expr: source.object_uri
    display_name: Object URI
    comment: s3://bucket/key for the object. Very high cardinality; drill-down only.
    synonyms: [s3 uri, s3 path, object path]

  # ---- who owns it -----------------------------------------------------------
  - name: Table Path
    expr: coalesce(source.table_uri, '(untracked)')
    display_name: Table Path
    comment: >-
      Normalized s3:// root of the table that owns the object (innermost table wins
      when tables nest), or "(untracked)" when no table owns it.
    synonyms: [table location, storage path, table root, table uri]
  - name: Table Name
    expr: coalesce(source.table_full_name, case when source.path_source = 'inferred_delta_log' then '(unregistered delta table)' else '(no table)' end)
    display_name: Table Name
    comment: Unity Catalog catalog.schema.table that owns the object; "(unregistered delta table)" for Delta data no UC table points at; "(no table)" otherwise.
    synonyms: [table, uc table, full table name]
  - name: Path Source
    expr: source.path_source
    display_name: Ownership
    comment: >-
      unity_catalog (a UC table owns it), inferred_delta_log (a Delta table with a
      _delta_log/ that no UC table owns -- often scratch output or a dropped table),
      or untracked (no table at all).
    synonyms: [ownership, owner type, registered, path type, tracked]
  - name: Table Activity
    expr: coalesce(source.activity_status, 'n/a')
    display_name: Table Activity
    comment: active when table lineage shows a read or write within s3_inventory_active_days of the snapshot, inactive otherwise; n/a for untracked data.
    synonyms: [activity, usage, active, inactive, unused, stale table, cold table]

  # ---- what it is --------------------------------------------------------------
  - name: Version State
    expr: source.version_state
    display_name: Version State
    comment: current (what listings show), noncurrent (an older version kept by versioning -- billed but invisible), or delete_marker (zero bytes).
    synonyms: [version, object version, noncurrent, old version, delete marker]
  - name: Object Kind
    expr: source.object_kind
    display_name: Object Kind
    comment: delta_log (files under _delta_log/), data_file (parquet / orc / avro), or other.
    synonyms: [file type, kind, object type]
  - name: Storage Class
    expr: source.storage_class
    display_name: Storage Class
    comment: S3 storage class (STANDARD, STANDARD_IA, INTELLIGENT_TIERING, GLACIER_IR, ...).
    synonyms: [tier, s3 tier, storage tier, class]
  - name: Pricing Tier
    expr: source.pricing_tier
    display_name: Pricing Tier
    comment: Storage class, or INTELLIGENT_TIERING:<access tier> for Intelligent-Tiering objects -- the key into the pricing seed.
    synonyms: [price tier, access tier]
  - name: Size Band
    expr: >-
      case
        when source.size_bytes < 131072 then '1. < 128 KB'
        when source.size_bytes < 8388608 then '2. 128 KB - 8 MB'
        when source.size_bytes < 134217728 then '3. 8 MB - 128 MB'
        when source.size_bytes < 1073741824 then '4. 128 MB - 1 GB'
        else '5. >= 1 GB'
      end
    display_name: Object Size Band
    comment: Object size bucket. Lots of objects under 128 KB means small-file problems (slow queries, minimum-size billing in IA / Glacier IR).
    synonyms: [object size, file size, small files, size bucket]

  # ---- when ---------------------------------------------------------------------
  - name: Object Age Band
    expr: >-
      case
        when source.age_days <= 30 then '1. 0-30 days'
        when source.age_days <= 90 then '2. 31-90 days'
        when source.age_days <= 365 then '3. 91-365 days'
        else '4. > 1 year'
      end
    display_name: Object Age Band
    comment: Days since the object version was last modified, measured at the snapshot date.
    synonyms: [age, object age, how old, cold data]
  - name: Last Modified Month
    expr: DATE_TRUNC('MONTH', source.last_modified_at)
    display_name: Last Modified Month
    comment: Month the object version was written.
    synonyms: [modified month, written month, created month]
    format:
      type: date
      date_format: locale_short_month
  - name: Snapshot Date
    expr: source.as_of_date
    display_name: Snapshot Date
    comment: Date of the bucket's latest inventory snapshot -- the "as of" date for every age and number.
    synonyms: [as of, inventory date, report date]
    format:
      type: date
      date_format: year_month_day
  - name: Inventory Source
    expr: source.inventory_source
    display_name: Inventory Source
    comment: s3_inventory for real data; sample when the project is running on its committed sample seeds.
    synonyms: [data source, sample data]

  # ---- savings --------------------------------------------------------------------
  - name: Savings Opportunity
    expr: coalesce(source.opportunity, 'none')
    display_name: Savings Opportunity
    comment: >-
      The single savings action this object version counts toward:
      expire_noncurrent_versions, remove_expired_delete_markers,
      archive_or_drop_inactive_tables, review_unregistered_data,
      fix_small_objects_in_min_size_classes, tier_cold_standard_data -- or none.
      Assigned in that priority order, so a byte is never counted twice.
    synonyms: [opportunity, savings, recommendation, action, waste, optimization]

measures:
  # ---- size -----------------------------------------------------------------------
  - name: Total Storage
    expr: COALESCE(SUM(source.size_bytes), 0)
    display_name: Total Storage
    comment: Bytes stored, all versions (current + noncurrent; delete markers are 0). What S3 bills for, before minimum-size rounding.
    synonyms: [size, total size, storage, bytes, footprint, how big]
    format:
      type: byte
      decimal_places:
        type: max
        places: 1
  - name: Current Storage
    expr: COALESCE(SUM(source.size_bytes) FILTER (WHERE source.version_state = 'current'), 0)
    display_name: Current Storage
    comment: Bytes in current versions -- what a bucket listing or a table scan sees.
    synonyms: [current size, live data, visible storage]
    format:
      type: byte
      decimal_places:
        type: max
        places: 1
  - name: Noncurrent Storage
    expr: COALESCE(SUM(source.size_bytes) FILTER (WHERE source.version_state = 'noncurrent'), 0)
    display_name: Noncurrent Storage
    comment: >-
      Bytes in noncurrent versions kept by bucket versioning: billed, invisible to
      listings and tables. On Delta tables this is what VACUUM / OPTIMIZE "deleted".
    synonyms: [noncurrent, old versions, hidden storage, versioned bytes, version overhead]
    format:
      type: byte
      decimal_places:
        type: max
        places: 1
  - name: Noncurrent Share
    expr: try_divide(MEASURE(`Noncurrent Storage`), MEASURE(`Total Storage`))
    display_name: Noncurrent Share
    comment: Share of stored bytes that are noncurrent versions. Above ~10% on a versioned bucket usually means no NoncurrentVersionExpiration lifecycle rule.
    synonyms: [version overhead percent, noncurrent percent]
    format:
      type: percentage
      decimal_places:
        type: max
        places: 1
  - name: Total Storage TiB
    expr: SUM(source.size_bytes) / POWER(1024, 4)
    display_name: Total Storage (TiB)
    comment: Total Storage as a plain number of TiB (2^40 bytes), for math and thresholds.
    synonyms: [tib, terabytes, tb]
    format:
      type: number
      decimal_places:
        type: max
        places: 3
  - name: Object Versions
    expr: COUNT(1)
    display_name: Object Versions
    comment: Rows -- object versions, including noncurrent versions and delete markers.
    synonyms: [rows, versions, version count]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Current Objects
    expr: COUNT_IF(source.version_state = 'current')
    display_name: Current Objects
    comment: Number of current object versions (what a listing returns).
    synonyms: [objects, files, file count, object count]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Delete Markers
    expr: COUNT_IF(source.version_state = 'delete_marker')
    display_name: Delete Markers
    comment: Delete markers. Zero bytes, but each one is a key that looks deleted while its older versions are still billed.
    synonyms: [deletes, tombstones]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Small Objects
    expr: COUNT_IF(source.version_state = 'current' AND source.size_bytes < 131072)
    display_name: Small Objects (< 128 KB)
    comment: Current objects under 128 KB. Small files slow queries and are billed as 128 KB in IA / Glacier IR.
    synonyms: [small files, tiny files]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Average Object Size
    expr: AVG(source.size_bytes) FILTER (WHERE source.version_state = 'current')
    display_name: Average Object Size
    comment: Mean size of current objects. For Delta / Parquet tables, far below ~128 MB means the table needs OPTIMIZE.
    synonyms: [avg file size, mean object size, file size]
    format:
      type: byte
      decimal_places:
        type: max
        places: 1
  - name: Buckets
    expr: COUNT(DISTINCT source.bucket)
    display_name: Buckets
    comment: Distinct buckets in the slice.
    synonyms: [bucket count, number of buckets]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Tables
    expr: COUNT(DISTINCT source.table_uri)
    display_name: Tables
    comment: Distinct table roots (UC tables + unregistered Delta tables) in the slice.
    synonyms: [table count, number of tables]
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

  # ---- money ------------------------------------------------------------------------
  - name: Monthly Storage Cost
    expr: SUM(source.monthly_cost_usd)
    display_name: Monthly Storage Cost (USD)
    comment: Storage list price per month at the pricing seed's rates. Not invoiced spend; excludes request, retrieval, transfer and transition charges.
    synonyms: [cost, spend, storage cost, monthly cost, bill, how much]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
  - name: Annual Storage Cost
    expr: SUM(source.monthly_cost_usd) * 12
    display_name: Annual Storage Cost (USD)
    comment: Monthly Storage Cost x 12.
    synonyms: [yearly cost, annual spend]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: Noncurrent Monthly Cost
    expr: COALESCE(SUM(source.monthly_cost_usd) FILTER (WHERE source.version_state = 'noncurrent'), 0)
    display_name: Noncurrent Monthly Cost (USD)
    comment: What noncurrent versions cost per month -- the price of versioning without an expiration rule.
    synonyms: [versioning cost, cost of old versions]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
  - name: Estimated Monthly Savings
    expr: COALESCE(SUM(source.estimated_monthly_savings_usd), 0)
    display_name: Est. Monthly Savings (USD)
    comment: >-
      Estimated monthly storage saving if every Savings Opportunity in the slice is
      acted on. Removals save the full cost; tiering and small-object fixes save the
      price gap. Adds up across slices (one opportunity per byte).
    synonyms: [savings, potential savings, waste, opportunity, how much can we save]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
  - name: Estimated Annual Savings
    expr: COALESCE(SUM(source.estimated_monthly_savings_usd), 0) * 12
    display_name: Est. Annual Savings (USD)
    comment: Estimated Monthly Savings x 12.
    synonyms: [yearly savings, annual savings]
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: Savings Share
    expr: try_divide(MEASURE(`Estimated Monthly Savings`), MEASURE(`Monthly Storage Cost`))
    display_name: Savings Share of Cost
    comment: Estimated Monthly Savings as a share of Monthly Storage Cost.
    synonyms: [savings percent, waste percent]
    format:
      type: percentage
      decimal_places:
        type: max
        places: 1
  - name: Storage With Savings
    expr: COALESCE(SUM(source.size_bytes) FILTER (WHERE source.opportunity IS NOT NULL), 0)
    display_name: Storage With a Savings Opportunity
    comment: Bytes that carry any Savings Opportunity.
    synonyms: [actionable storage, wasted bytes]
    format:
      type: byte
      decimal_places:
        type: max
        places: 1
