{{ config(enabled = s3_inventory_use_sample_data()) }}
{#-
  Correctness on the committed sample (sample mode only): every savings opportunity,
  its bytes, and its estimated monthly saving at the seeded us-east-1 list prices.
  Notable negatives this also proves:
    * orders/part-00000 has a noncurrent version only 5 days noncurrent (its delete
      marker is recent) -> NOT expired, despite the version itself being 75 days old
    * that delete marker still protects a version -> NOT an expired delete marker
    * the 4 KB Delta log JSON is STANDARD and old, but under 128 KB -> NOT tiered
    * the Intelligent-Tiering object is already tiered -> no opportunity
  Savings compare with a tolerance; bytes and counts compare exactly.
-#}
with expected (bucket, opportunity, table_path, object_count, bytes, estimated_monthly_savings_usd) as (
    values
        ('acme-lakehouse-prod', 'expire_noncurrent_versions',            's3://acme-lakehouse-prod/tables/sales/orders',        1,  32212254720, 0.69),
        ('acme-lakehouse-prod', 'remove_expired_delete_markers',         '(untracked)',                                         1,            0, 0.0),
        ('acme-lakehouse-prod', 'archive_or_drop_inactive_tables',       's3://acme-lakehouse-prod/tables/sales/legacy_orders', 2, 322122549248, 6.900000044),
        ('acme-lakehouse-prod', 'review_unregistered_data',              's3://acme-lakehouse-prod/scratch/tmp_join',           2,  85899346944, 1.840000022),
        ('acme-lakehouse-prod', 'review_unregistered_data',              '(untracked)',                                         1,  21474836480, 0.46),
        ('acme-lakehouse-prod', 'tier_cold_standard_data',               's3://acme-lakehouse-prod/tables/sales/orders',        2, 246960619520, 2.414995),
        ('acme-logs',           'fix_small_objects_in_min_size_classes', '(untracked)',                                         1,        10240, 0.000001307),
        ('acme-logs',           'tier_cold_standard_data',               '(untracked)',                                         1, 536870912000, 5.2499975),
        ('acme-archive',        'fix_small_objects_in_min_size_classes', '(untracked)',                                         1,         4096, 0.000000401),
        ('acme-scratch-current-only', 'tier_cold_standard_data',         '(untracked)',                                         1, 549755813888, 5.3759975)
),

actual as (
    select bucket, opportunity, table_path, object_count, bytes, estimated_monthly_savings_usd
    from {{ ref('s3_storage_savings_opportunities') }}
)

select
    coalesce(expected.bucket, actual.bucket)               as bucket,
    coalesce(expected.opportunity, actual.opportunity)     as opportunity,
    coalesce(expected.table_path, actual.table_path)       as table_path,
    expected.object_count                  as expected_object_count,
    actual.object_count                    as actual_object_count,
    expected.bytes                         as expected_bytes,
    actual.bytes                           as actual_bytes,
    expected.estimated_monthly_savings_usd as expected_savings,
    actual.estimated_monthly_savings_usd   as actual_savings
from expected
full outer join actual
    on  actual.bucket      = expected.bucket
    and actual.opportunity = expected.opportunity
    and actual.table_path  = expected.table_path
where expected.bucket is null
   or actual.bucket is null
   or actual.object_count != expected.object_count
   or actual.bytes        != expected.bytes
   or abs(actual.estimated_monthly_savings_usd - expected.estimated_monthly_savings_usd) > 0.000001
