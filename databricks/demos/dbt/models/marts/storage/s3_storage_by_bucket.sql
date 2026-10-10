{#-
  One row per bucket: size, versioning status, cost, and savings headroom as of the
  bucket's latest inventory snapshot.

  Versioning status is INFERRED from the inventory's version fields (S3 Inventory
  does not report the bucket setting itself):
    unknown    -- no version fields (is_latest / is_delete_marker) at all: the report
                  lists current versions only. Reconfigure it with "Include all
                  versions" to classify the bucket.
    disabled   -- version fields present, but no object has a real version id:
                  versioning was never enabled.
    suspended  -- real version ids exist, but the newest current object has a null
                  version id: versioning was enabled, then suspended.
    enabled    -- otherwise.
-#}
with objects as (
    select * from {{ ref('int_s3_inventory__objects_classified') }}
),

rollup as (

    select
        bucket,
        max(as_of_date)        as as_of_date,
        max(inventory_snapshot) as inventory_snapshot,
        max(ingested_at)        as inventory_ingested_at,
        max(inventory_source)  as inventory_source,
        bool_or(is_table_bucket) as is_table_bucket,

        -- versioning evidence
        bool_or(has_version_field)   as has_version_fields,
        bool_or(has_real_version_id) as has_real_version_ids,
        max_by(has_real_version_id, last_modified_at)
            filter (where version_state = 'current') as newest_current_has_real_version_id,

        -- sizes
        count_if(version_state = 'current')                              as current_object_count,
        coalesce(sum(size_bytes) filter (where version_state = 'current'), 0)    as current_bytes,
        count_if(version_state = 'noncurrent')                           as noncurrent_version_count,
        coalesce(sum(size_bytes) filter (where version_state = 'noncurrent'), 0) as noncurrent_bytes,
        count_if(version_state = 'delete_marker')                        as delete_marker_count,
        coalesce(sum(size_bytes), 0)                                     as total_bytes,

        -- where the current bytes live
        coalesce(sum(size_bytes) filter (where version_state = 'current' and path_source = 'unity_catalog'), 0)        as uc_table_bytes,
        coalesce(sum(size_bytes) filter (where version_state = 'current' and path_source = 'inferred_delta_log'), 0)   as unregistered_delta_bytes,
        coalesce(sum(size_bytes) filter (where version_state = 'current' and path_source = 'untracked'), 0)            as untracked_bytes,

        -- money (storage list price, USD / month)
        sum(monthly_cost_usd)                                                as monthly_cost_usd,
        coalesce(sum(monthly_cost_usd) filter (where version_state = 'noncurrent'), 0) as noncurrent_monthly_cost_usd,
        coalesce(sum(estimated_monthly_savings_usd), 0)                      as estimated_monthly_savings_usd

    from objects
    group by bucket

)

select
    bucket,
    as_of_date,
    inventory_snapshot,
    inventory_ingested_at,
    inventory_source,
    case
        when not has_version_fields   then 'unknown'
        when not has_real_version_ids then 'disabled'
        when not coalesce(newest_current_has_real_version_id, true) then 'suspended'
        else 'enabled'
    end as versioning_status,
    case
        when not has_version_fields then null
        else has_real_version_ids and coalesce(newest_current_has_real_version_id, true)
    end as versioning_enabled,
    is_table_bucket,

    current_object_count,
    current_bytes,
    noncurrent_version_count,
    noncurrent_bytes,
    delete_marker_count,
    total_bytes,
    round(total_bytes / power(1024, 4), 6) as total_tib,
    try_divide(noncurrent_bytes, total_bytes) as noncurrent_share,

    uc_table_bytes,
    unregistered_delta_bytes,
    untracked_bytes,

    monthly_cost_usd,
    noncurrent_monthly_cost_usd,
    estimated_monthly_savings_usd

from rollup
