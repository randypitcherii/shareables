{#-
  Ranked storage savings opportunities. Grain: one row per bucket + opportunity +
  table_path ('(untracked)' when no table owns the bytes).

  Each object version carries at most one opportunity (see
  int_s3_inventory__objects_classified), so estimated savings add up across rows
  without double counting. Figures are monthly S3 STORAGE list-price estimates from
  the s3_storage_class_pricing seed -- not invoiced spend, and excluding request,
  transition, and early-deletion charges.
-#}
with objects as (
    select * from {{ ref('int_s3_inventory__objects_classified') }}
    where opportunity is not null
),

rollup as (
    select
        bucket,
        opportunity,
        coalesce(table_uri, '(untracked)') as table_path,
        max(table_full_name)               as table_full_name,
        max(path_source)                   as path_source,
        count(*)                           as object_count,
        sum(size_bytes)                    as bytes,
        sum(monthly_cost_usd)              as monthly_cost_usd,
        sum(estimated_monthly_savings_usd) as estimated_monthly_savings_usd
    from objects
    group by bucket, opportunity, coalesce(table_uri, '(untracked)')
)

select
    bucket,
    opportunity,
    case opportunity
        when 'expire_noncurrent_versions'
            then 'Add a lifecycle rule with NoncurrentVersionExpiration (and keep VACUUM retention aligned) so deleted/overwritten versions stop accruing.'
        when 'remove_expired_delete_markers'
            then 'Add a lifecycle rule with ExpiredObjectDeleteMarker: true to clean up delete markers that protect no versions.'
        when 'archive_or_drop_inactive_tables'
            then 'Confirm with the table owner, then DROP (and purge) the table or move its data to an archive storage class.'
        when 'review_unregistered_data'
            then 'No Unity Catalog table owns this data: register it if it is needed, otherwise delete it.'
        when 'fix_small_objects_in_min_size_classes'
            then 'Objects under 128 KB are billed as 128 KB in IA / Glacier IR: keep them in Standard (lifecycle ObjectSizeGreaterThan filter) or compact them.'
        when 'tier_cold_standard_data'
            then 'Transition to Intelligent-Tiering so cold objects move to cheaper access tiers automatically without retrieval fees.'
    end as recommended_action,
    table_path,
    table_full_name,
    path_source,
    object_count,
    bytes,
    round(bytes / power(1024, 3), 6)        as gib,
    monthly_cost_usd,
    estimated_monthly_savings_usd,
    estimated_monthly_savings_usd * 12      as estimated_annual_savings_usd,
    dense_rank() over (order by estimated_monthly_savings_usd desc) as savings_rank
from rollup
