{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/warehouse_events.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1

# Fact: system.compute.warehouse_events (one SQL warehouse state event).
# Grain (workspace_id, warehouse_id, event_time, event_type). Inline SCD
# warehouse join, point-in-time on event_time. Validated N:1.
source: {{ source('system_compute', 'warehouse_events') }}

comment: SQL warehouse state events (scaling/start/stop) with point-in-time warehouse context.

joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  - name: warehouse
    source: |
      SELECT workspace_id, warehouse_id, warehouse_name, warehouse_type, warehouse_size, change_time,
             lead(change_time) OVER (PARTITION BY workspace_id, warehouse_id ORDER BY change_time) AS next_change_time
      FROM {{ source('system_compute', 'warehouses') }}
    "on": warehouse.workspace_id = source.workspace_id
      AND warehouse.warehouse_id = source.warehouse_id
      AND source.event_time >= warehouse.change_time
      AND (source.event_time < warehouse.next_change_time OR warehouse.next_change_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

dimensions:
  - name: Event Time
    expr: source.event_time
    comment: Timestamp of when the event took place in UTC.
  - name: Event Date
    expr: CAST(source.event_time AS DATE)
    comment: Calendar date of the event.
    format:
      type: date
      date_format: year_month_day
  - name: Workspace ID
    expr: source.workspace_id
    comment: The ID of the workspace where the warehouse is deployed.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Warehouse ID
    expr: source.warehouse_id
    comment: The ID of the SQL warehouse the event is related to.
  - name: Warehouse Name
    expr: warehouse.warehouse_name
    comment: The name of the SQL warehouse.
  - name: Warehouse Type
    expr: warehouse.warehouse_type
    comment: The type of the SQL warehouse.
  - name: Warehouse Size
    expr: warehouse.warehouse_size
    comment: The cluster size of the SQL warehouse (e.g. SMALL, MEDIUM, LARGE).
  - name: Event Type
    expr: source.event_type
    comment: 'The type of warehouse event. Possible values: SCALED_UP, SCALED_DOWN, STOPPING, RUNNING, STARTING, STOPPED.'

measures:
  - name: Events
    expr: COUNT(1)
    comment: Number of warehouse events in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Warehouses
    expr: COUNT(DISTINCT source.warehouse_id)
    comment: Distinct number of warehouses in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Avg Active Clusters
    expr: AVG(source.cluster_count)
    comment: Average number of actively running clusters at event time.
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: Max Active Clusters
    expr: MAX(source.cluster_count)
    comment: Maximum number of actively running clusters observed.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Scale Up Events
    expr: COUNT(1) FILTER (WHERE source.event_type = 'SCALED_UP')
    comment: Number of scale-up events.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Scale Down Events
    expr: COUNT(1) FILTER (WHERE source.event_type = 'SCALED_DOWN')
    comment: Number of scale-down events.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
