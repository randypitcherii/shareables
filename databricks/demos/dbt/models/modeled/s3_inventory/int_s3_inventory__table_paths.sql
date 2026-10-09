{#-
  Every S3 location that holds a table, with its activity status. Grain: one row
  per table_uri.

  Two kinds of table root:
    * unity_catalog       -- the storage_path of a UC table. Several UC tables can
                             share one location (re-registrations, external tables
                             over the same data); they collapse to one root here,
                             reporting the first name and how many share it.
    * inferred_delta_log  -- a prefix with a `_delta_log/` directory that NO UC table
                             points at (or sits under): an unregistered Delta table.
                             Usually scratch output, abandoned clones, or tables
                             dropped from UC whose data was never deleted.

  Activity = most recent read or write in table lineage, by table name for UC roots
  and by path for both kinds. A root is `active` when it was touched within
  `s3_inventory_active_days` of its bucket's snapshot date.
-#}
{%- set active_days = var('s3_inventory_active_days', 90) | int -%}

with objects as (
    select bucket, key, as_of_date from {{ ref('int_s3_inventory__objects') }}
),

bucket_as_of as (
    select bucket, max(as_of_date) as as_of_date
    from objects
    group by bucket
),

uc_roots as (
    select
        table_uri,
        bucket,
        min(table_full_name)        as table_full_name,
        count(*)                    as registered_table_count,
        min(table_type)             as table_type,
        min(data_source_format)     as data_source_format,
        'unity_catalog'             as path_source
    from {{ ref('stg_s3_inventory__uc_tables') }}
    group by table_uri, bucket
),

-- non-greedy: the OUTERMOST _delta_log wins, so a table's checkpoint subdirs never
-- become their own "table"
delta_log_roots as (
    select distinct
        bucket,
        {{ s3_inventory_normalize_uri("concat('s3://', bucket, '/', regexp_extract(key, '^(.*?)/_delta_log/', 1))") }} as table_uri
    from objects
    where key rlike '^.+?/_delta_log/'
),

inferred_roots as (
    select
        delta_log_roots.table_uri,
        delta_log_roots.bucket,
        cast(null as string)  as table_full_name,
        0                     as registered_table_count,
        cast(null as string)  as table_type,
        'DELTA'               as data_source_format,
        'inferred_delta_log'  as path_source
    from delta_log_roots
    left anti join uc_roots
        on  uc_roots.bucket = delta_log_roots.bucket
        and (   delta_log_roots.table_uri = uc_roots.table_uri
             or startswith(delta_log_roots.table_uri, concat(uc_roots.table_uri, '/')))
),

roots as (
    select * from uc_roots
    union all
    select * from inferred_roots
),

events as (
    select * from {{ ref('stg_s3_inventory__table_access_events') }}
),

-- name-matched activity (UC tables)
name_activity as (
    select
        roots.table_uri,
        max(case when events.access_type = 'read'  then events.event_time end) as last_read_at,
        max(case when events.access_type = 'write' then events.event_time end) as last_write_at
    from roots
    inner join {{ ref('stg_s3_inventory__uc_tables') }} as uc_tables
        on uc_tables.table_uri = roots.table_uri
    inner join events
        on events.table_full_name = uc_tables.table_full_name
    group by roots.table_uri
),

-- path-matched activity (direct s3:// reads/writes at or under the root)
path_activity as (
    select
        roots.table_uri,
        max(case when events.access_type = 'read'  then events.event_time end) as last_read_at,
        max(case when events.access_type = 'write' then events.event_time end) as last_write_at
    from roots
    inner join events
        on  events.path_uri is not null
        and (   events.path_uri = roots.table_uri
             or startswith(events.path_uri, concat(roots.table_uri, '/')))
    group by roots.table_uri
),

activity as (
    select
        roots.*,
        bucket_as_of.as_of_date,
        greatest(name_activity.last_read_at,  path_activity.last_read_at)  as last_read_at,
        greatest(name_activity.last_write_at, path_activity.last_write_at) as last_write_at
    from roots
    left join name_activity on name_activity.table_uri = roots.table_uri
    left join path_activity on path_activity.table_uri = roots.table_uri
    -- only roots in inventoried buckets: a table on an uninventoried bucket has no
    -- storage to attribute here
    inner join bucket_as_of on bucket_as_of.bucket     = roots.bucket
)

select
    table_uri,
    bucket,
    regexp_extract(table_uri, '^s3://[^/]+/?(.*)$', 1) as table_prefix,
    table_full_name,
    registered_table_count,
    table_type,
    data_source_format,
    path_source,
    as_of_date,
    last_read_at,
    last_write_at,
    greatest(last_read_at, last_write_at)                                   as last_access_at,
    datediff(as_of_date, cast(greatest(last_read_at, last_write_at) as date)) as days_since_last_access,
    case
        when greatest(last_read_at, last_write_at) >= date_sub(as_of_date, {{ active_days }})
        then 'active'
        else 'inactive'
    end as activity_status

from activity
