{#-
  Should this file be here? Every CURRENT object under a Delta table root, checked
  against the files the table's transaction log references. Grain: one row per
  current object version under a Delta root.

  The file lists come from the pipeline's one Python model
  (int_s3_inventory__delta_snapshot_files) -- or its sample seed. Everything from
  here on is SQL.

  delta_file_status, first match wins:
    delta_metadata    a path segment starts with _ or . (_delta_log/, _change_data/,
                      checkpoints, ...) or a deletion-vector file. VACUUM leaves
                      these to Delta itself; reported, never flagged.
    not_checked       the table's log couldn't be read (no access, dropped, ...).
    active            in the CURRENT snapshot of ANY table. "Any" matters: a
                      shallow clone references files under its SOURCE table's path,
                      so a file the source dropped can still be live.
    retained          removed inside the retention window (it is in a `window`
                      snapshot), or written after the horizon (in-flight writes).
                      Time travel needs it: VACUUM must keep it.
    unreferenced      not referenced, older than the horizon, but the table's
                      FIRST in-window commit removed files that can't be named
                      (Databricks blocks time travel past the retention), or the
                      window was truncated. Might still be retained: unproven.
    vacuum_eligible   referenced by nothing, and provably not removed inside the
                      window. VACUUM would delete it now -- it shouldn't be here.

  horizon = now - delta.deletedFileRetentionDuration, evaluated when the file lists
  were taken: the question is "what would VACUUM delete if run now?".
-#}
{%- if s3_inventory_use_sample_data() -%}
  {%- set snapshot_files = ref('s3_inventory_sample__delta_snapshot_files') -%}
{%- else -%}
  {%- set snapshot_files = ref('int_s3_inventory__delta_snapshot_files') -%}
{%- endif -%}

with snapshot_files as (
    select
        {{ s3_inventory_normalize_uri('table_uri') }} as table_uri,
        table_full_name,
        snapshot_kind,
        -- log paths can come back URL-encoded (partition values); inventory keys don't
        {{ s3_inventory_normalize_uri("coalesce(try_url_decode(file_uri), file_uri)") }} as file_uri,
        snapshot_at,
        horizon_at,
        retention_duration,
        window_ambiguous_removed_files,
        list_status
    from {{ snapshot_files }}
),

roots as (
    select
        table_uri,
        regexp_extract(table_uri, '^s3://([^/]+)', 1) as bucket,
        max(table_full_name)    as table_full_name,
        max(snapshot_at)        as snapshot_at,
        max(horizon_at)         as horizon_at,
        max(retention_duration) as retention_duration,
        bool_or(list_status like 'error%')               as list_failed,
        -- leftovers can't be proven unreferenced when the first in-window commit
        -- removed files (count known, names not) or the window was truncated
        coalesce(max(window_ambiguous_removed_files), 0) != 0
            or bool_or(list_status = 'window_truncated') as window_unproven
    from snapshot_files
    group by table_uri
),

-- live anywhere = in the current snapshot of any table (clones included)
active_files as (
    select file_uri, count(distinct table_uri) as referencing_tables
    from snapshot_files
    where snapshot_kind = 'current' and file_uri is not null
    group by file_uri
),

-- files removed inside the retention window: VACUUM must keep them
window_files as (
    select distinct table_uri, file_uri
    from snapshot_files
    where snapshot_kind = 'window' and file_uri is not null
),

own_current_files as (
    select distinct table_uri, file_uri
    from snapshot_files
    where snapshot_kind = 'current' and file_uri is not null
),

objects as (
    select bucket, key, version_id, object_uri, prefix, size_bytes, last_modified_at
    from {{ ref('int_s3_inventory__objects') }}
    where version_state = 'current'
),

-- innermost Delta root per prefix (matched once per distinct prefix, bucket-bounded)
prefix_root as (
    select prefixes.bucket, prefixes.prefix, roots.table_uri
    from (
        select distinct bucket, prefix,
               {{ s3_inventory_normalize_uri("concat('s3://', bucket, '/', prefix)") }} as prefix_uri
        from objects
    ) as prefixes
    inner join roots
        on  roots.bucket = prefixes.bucket
        and (prefixes.prefix_uri = roots.table_uri or startswith(prefixes.prefix_uri, concat(roots.table_uri, '/')))
    qualify row_number() over (partition by prefixes.bucket, prefixes.prefix order by length(roots.table_uri) desc) = 1
),

checked as (
    select
        objects.*,
        roots.table_uri,
        roots.table_full_name,
        roots.snapshot_at,
        roots.horizon_at,
        roots.retention_duration,
        substr(objects.object_uri, length(roots.table_uri) + 2) as relative_path,
        roots.list_failed,
        roots.window_unproven,
        active_files.file_uri is not null        as is_active_anywhere,
        own_current_files.file_uri is not null   as is_active_in_own_table,
        window_files.file_uri is not null        as is_removed_in_window
    from objects
    inner join prefix_root
        on prefix_root.bucket = objects.bucket and prefix_root.prefix = objects.prefix
    inner join roots
        on roots.table_uri = prefix_root.table_uri
    left join active_files
        on active_files.file_uri = objects.object_uri
    left join own_current_files
        on own_current_files.table_uri = roots.table_uri and own_current_files.file_uri = objects.object_uri
    left join window_files
        on window_files.table_uri = roots.table_uri and window_files.file_uri = objects.object_uri
)

select
    bucket,
    key,
    version_id,
    object_uri,
    table_uri,
    table_full_name,
    relative_path,
    size_bytes,
    last_modified_at,
    snapshot_at,
    horizon_at,
    retention_duration,
    is_active_anywhere,
    -- live only because ANOTHER table (a shallow clone) still references it:
    -- VACUUM on that clone's source would break the clone
    is_active_anywhere and not is_active_in_own_table as is_kept_alive_by_other_table,
    case
        when relative_path rlike '(^|/)[_.]'
          or regexp_extract(relative_path, '[^/]+$', 0) like 'deletion_vector_%' then 'delta_metadata'
        when list_failed                                                         then 'not_checked'
        when is_active_anywhere                                                  then 'active'
        when is_removed_in_window or last_modified_at >= horizon_at              then 'retained'
        when window_unproven                                                     then 'unreferenced'
        else                                                                          'vacuum_eligible'
    end as delta_file_status
from checked
