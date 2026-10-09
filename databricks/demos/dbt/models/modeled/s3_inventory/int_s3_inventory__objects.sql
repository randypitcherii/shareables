{#-
  The latest inventory snapshot per bucket, one row per object VERSION, enriched
  with everything the marts aggregate on: version state, prefix, object kind, age,
  how long a version has been noncurrent, and its priced monthly storage cost.

  Why latest-per-bucket and not one global max(dt): each bucket's inventory is a
  separate configuration delivered on its own schedule, so a single global max would
  silently drop every bucket whose newest report landed a few minutes earlier.

  Ages are measured from the SNAPSHOT date (as_of_date), not current_date(): an
  object is as old as the inventory says it was when listed, and the results are
  reproducible no matter when the model runs.
-#}
with objects as (
    select * from {{ ref('stg_s3_inventory__objects') }}
),

latest_snapshot as (
    select bucket, max(inventory_snapshot) as inventory_snapshot
    from objects
    group by bucket
),

snapshot_objects as (
    select objects.*
    from objects
    inner join latest_snapshot
        on  objects.bucket = latest_snapshot.bucket
        -- null-safe: inventories registered without the `dt` partition are one snapshot
        and objects.inventory_snapshot <=> latest_snapshot.inventory_snapshot
),

pricing as (
    select * from {{ ref('s3_storage_class_pricing') }}
),

versioned as (

    select
        bucket,
        key,
        version_id,
        size_bytes,
        last_modified_at,
        storage_class,
        intelligent_tiering_access_tier,
        is_multipart_uploaded,
        inventory_snapshot,
        inventory_source,
        coalesce(inventory_snapshot_date, current_date()) as as_of_date,

        coalesce(is_delete_marker, false) as is_delete_marker,
        -- current-version-only reports have no is_latest: everything listed is current
        coalesce(is_latest, true)         as is_latest,

        -- An "all versions" report populates is_latest / is_delete_marker on EVERY
        -- row; a current-version-only report omits them. That -- not version_id --
        -- is the reliable signal that versioning state is observable at all, because
        -- objects written while versioning was off or suspended carry a "null" version
        -- id that can arrive as NULL, '' or the literal string 'null'.
        (is_latest is not null or is_delete_marker is not null) as has_version_field,
        coalesce(version_id not in ('', 'null'), false)         as has_real_version_id,

        -- a version becomes noncurrent when the next-newer version (or delete marker)
        -- of the same key is written -- exactly how lifecycle NoncurrentDays counts
        lead(last_modified_at) over (
            partition by bucket, key
            order by last_modified_at, coalesce(is_latest, true)
        ) as noncurrent_since_at,

        count(*) over (partition by bucket, key) as key_version_count

    from snapshot_objects

),

enriched as (

    select
        versioned.*,

        concat('s3://', bucket, '/', key) as object_uri,

        -- the "directory" an object sits in; '' for objects at the bucket root
        coalesce(regexp_extract(key, '^(.*)/[^/]*$', 1), '') as prefix,

        case
            when is_delete_marker then 'delete_marker'
            when is_latest        then 'current'
            else                       'noncurrent'
        end as version_state,

        case
            when key rlike '(^|/)_delta_log/'                       then 'delta_log'
            when lower(key) rlike '\\.(parquet|orc|avro)(\\.[a-z0-9]+)?$' then 'data_file'
            else                                                         'other'
        end as object_kind,

        -- a delete marker with no older versions behind it protects nothing
        (is_delete_marker and is_latest and key_version_count = 1) as is_expired_delete_marker,

        datediff(as_of_date, cast(last_modified_at as date)) as age_days,
        case
            when not is_latest and not is_delete_marker
            then datediff(as_of_date, cast(noncurrent_since_at as date))
        end as noncurrent_days,

        case
            when storage_class = 'INTELLIGENT_TIERING'
            then concat('INTELLIGENT_TIERING:', coalesce(intelligent_tiering_access_tier, 'FREQUENT'))
            else storage_class
        end as pricing_tier

    from versioned

)

select
    enriched.*,

    pricing.price_per_gb_month_usd,
    coalesce(pricing.min_billable_object_bytes, 0) as min_billable_object_bytes,

    -- IA / Glacier IR bill small objects as 128 KB; delete markers are not billed
    case
        when is_delete_marker then 0
        else greatest(size_bytes, coalesce(pricing.min_billable_object_bytes, 0))
    end as billable_bytes,

    -- storage only; NULL when the storage class is missing from the pricing seed
    case
        when is_delete_marker then 0
        else {{ s3_inventory_bytes_to_gib('greatest(size_bytes, coalesce(pricing.min_billable_object_bytes, 0))') }}
             * pricing.price_per_gb_month_usd
             -- Intelligent-Tiering's per-object monitoring fee (objects >= 128 KB only)
             + case when size_bytes >= 131072 then coalesce(pricing.monthly_fee_per_1000_objects_usd, 0) / 1000.0 else 0 end
    end as monthly_cost_usd

from enriched
left join pricing
    on enriched.pricing_tier = pricing.pricing_tier
