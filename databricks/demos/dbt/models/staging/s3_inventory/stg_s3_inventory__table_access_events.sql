{#-
  Read / write events per table AND per path, unpivoted from table lineage: the
  source side of a lineage event is a read, the target side a write. Path events
  (direct `s3://...` access, no UC table) are kept so unregistered Delta tables can
  still be judged active or inactive.

  Real mode scans only the activity window (+7 days of slack for snapshot lag) of
  system.access.table_lineage; sample mode reads the committed sample seed.
-#}
{%- set use_sample = s3_inventory_use_sample_data() -%}
{%- set lookback_days = (var('s3_inventory_active_days', 90) | int) + 7 -%}

with source as (
    {% if use_sample -%}
    select * from {{ ref('s3_inventory_sample__table_lineage') }}
    {%- else -%}
    select event_time, source_table_full_name, source_path, target_table_full_name, target_path
    from {{ source('system_access', 'table_lineage') }}
    where event_date >= current_date() - interval {{ lookback_days }} days
    {%- endif %}
),

unpivoted as (

    select
        event_time,
        source_table_full_name as table_full_name,
        source_path            as path,
        'read'                 as access_type
    from source
    where source_table_full_name is not null or source_path is not null

    union all

    select
        event_time,
        target_table_full_name as table_full_name,
        target_path            as path,
        'write'                as access_type
    from source
    where target_table_full_name is not null or target_path is not null

)

select
    event_time,
    cast(event_time as date) as event_date,
    table_full_name,
    case
        when lower(path) rlike '^s3[an]?://' then {{ s3_inventory_normalize_uri('path') }}
    end as path_uri,
    access_type
from unpivoted
