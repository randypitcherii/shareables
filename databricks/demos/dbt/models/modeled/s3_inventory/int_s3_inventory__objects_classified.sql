{#-
  Every object version from the latest snapshot, attributed to the table that owns
  it and assigned AT MOST ONE storage-savings opportunity. Grain: one row per object
  version (same as int_s3_inventory__objects).

  Table attribution: each object's prefix is matched to the LONGEST table root that
  contains it (nested roots resolve to the innermost table). Matching happens once
  per distinct (bucket, prefix) -- far fewer rows than objects -- with a bucket
  equi-join bounding the prefix comparison.

  Opportunities are assigned in priority order, so each byte is counted once and the
  per-opportunity savings in the mart add up without double counting:

    1. expire_noncurrent_versions        noncurrent > s3_inventory_noncurrent_retention_days
    2. remove_expired_delete_markers     delete marker with nothing behind it
    3. archive_or_drop_inactive_tables   current data under an INACTIVE UC table
    4. review_unregistered_data          current data in a table bucket that no UC table owns
    5. fix_small_objects_in_min_size_classes  < 128 KB objects billed as 128 KB (IA / Glacier IR)
    6. tier_cold_standard_data           STANDARD, >= s3_inventory_cold_days old, >= 128 KB

  Savings are monthly STORAGE list-price estimates: removal-type opportunities (1, 3,
  4) save the object's full cost; 5 saves the gap to S3 Standard at true size; 6
  saves the gap to Intelligent-Tiering's Infrequent tier, net of its monitoring fee.
  Transition and request charges are not modeled.
-#}
{%- set cold_days = var('s3_inventory_cold_days', 90) | int -%}
{%- set noncurrent_retention_days = var('s3_inventory_noncurrent_retention_days', 30) | int -%}

with objects as (
    select * from {{ ref('int_s3_inventory__objects') }}
),

table_paths as (
    select * from {{ ref('int_s3_inventory__table_paths') }}
),

-- buckets that host at least one UC table: only there is "no table owns this" a
-- signal. In a log / backup bucket, everything is untracked by design.
table_buckets as (
    select distinct bucket
    from table_paths
    where path_source = 'unity_catalog'
),

prefixes as (
    select distinct
        bucket,
        prefix,
        {{ s3_inventory_normalize_uri("concat('s3://', bucket, '/', prefix)") }} as prefix_uri
    from objects
),

prefix_owner as (
    select
        prefixes.bucket,
        prefixes.prefix,
        table_paths.table_uri
    from prefixes
    inner join table_paths
        on  table_paths.bucket = prefixes.bucket
        and (   prefixes.prefix_uri = table_paths.table_uri
             or startswith(prefixes.prefix_uri, concat(table_paths.table_uri, '/')))
    qualify row_number() over (
        partition by prefixes.bucket, prefixes.prefix
        order by length(table_paths.table_uri) desc
    ) = 1
),

targets as (
    -- price points the tiering / small-object savings are measured against
    select
        max(case when pricing_tier = 'STANDARD' then price_per_gb_month_usd end)                         as standard_price,
        max(case when pricing_tier = 'INTELLIGENT_TIERING:INFREQUENT' then price_per_gb_month_usd end)   as it_infrequent_price,
        max(case when pricing_tier = 'INTELLIGENT_TIERING:INFREQUENT' then monthly_fee_per_1000_objects_usd end) as it_fee_per_1000
    from {{ ref('s3_storage_class_pricing') }}
),

attributed as (

    select
        objects.*,
        table_paths.table_uri,
        table_paths.table_full_name,
        coalesce(table_paths.path_source, 'untracked') as path_source,
        table_paths.activity_status,
        table_buckets.bucket is not null               as is_table_bucket
    from objects
    left join prefix_owner
        on  prefix_owner.bucket = objects.bucket
        and prefix_owner.prefix = objects.prefix
    left join table_paths
        on table_paths.table_uri = prefix_owner.table_uri
    left join table_buckets
        on table_buckets.bucket = objects.bucket

),

classified as (

    select
        attributed.*,
        case
            when version_state = 'noncurrent'
                 and noncurrent_days > {{ noncurrent_retention_days }}
                then 'expire_noncurrent_versions'
            when is_expired_delete_marker
                then 'remove_expired_delete_markers'
            when version_state != 'current'
                then null
            when path_source = 'unity_catalog' and activity_status = 'inactive'
                then 'archive_or_drop_inactive_tables'
            when is_table_bucket and path_source in ('inferred_delta_log', 'untracked')
                then 'review_unregistered_data'
            when min_billable_object_bytes > size_bytes
                then 'fix_small_objects_in_min_size_classes'
            when storage_class = 'STANDARD'
                 and age_days >= {{ cold_days }}
                 and size_bytes >= 131072
                then 'tier_cold_standard_data'
        end as opportunity
    from attributed

)

select
    classified.*,
    case
        when opportunity in ('expire_noncurrent_versions', 'archive_or_drop_inactive_tables', 'review_unregistered_data')
            then monthly_cost_usd
        when opportunity = 'fix_small_objects_in_min_size_classes'
            then greatest(monthly_cost_usd - {{ s3_inventory_bytes_to_gib('size_bytes') }} * targets.standard_price, 0)
        when opportunity = 'tier_cold_standard_data'
            then greatest(
                monthly_cost_usd
                - ({{ s3_inventory_bytes_to_gib('size_bytes') }} * targets.it_infrequent_price
                   + targets.it_fee_per_1000 / 1000.0),
                0)
        when opportunity is not null
            then 0
    end as estimated_monthly_savings_usd
from classified
cross join targets
