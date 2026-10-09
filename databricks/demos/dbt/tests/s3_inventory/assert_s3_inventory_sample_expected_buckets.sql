{{ config(enabled = s3_inventory_use_sample_data()) }}
{#-
  Correctness on the committed sample (sample mode only): exact expected size and
  versioning classification per bucket. Exercises latest-snapshot selection (the
  1000 TiB "ghost" object sits in an older snapshot and must not count), every
  versioning state, and delete markers.

  Returns rows that differ in either direction (missing, extra, or wrong).
-#}
with expected (bucket, versioning_status, current_bytes, noncurrent_bytes, delete_marker_count) as (
    values
        ('acme-lakehouse-prod',       'enabled',   794568956928, 85899345920, 2),
        ('acme-logs',                 'disabled', 1743756732416,           0, 0),
        ('acme-archive',              'suspended', 2199023259648,          0, 0),
        ('acme-scratch-current-only', 'unknown',   1649267441664,          0, 0)
),

actual as (
    select bucket, versioning_status, current_bytes, noncurrent_bytes, delete_marker_count
    from {{ ref('s3_storage_by_bucket') }}
)

(select 'missing or wrong' as problem, * from expected except select 'missing or wrong', * from actual)
union all
(select 'unexpected' as problem, * from actual except select 'unexpected', * from expected)
