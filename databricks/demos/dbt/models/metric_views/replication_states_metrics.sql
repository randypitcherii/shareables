{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/standalone_facts.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1
source: {{ source('system_replication', 'states') }}
comment: Replication heartbeat states (account-level standalone fact, Private Preview).
dimensions:
  - name: Event Time
    expr: source.event_time
    comment: When the event was emitted.
  - name: Event Date
    expr: CAST(source.event_time AS DATE)
    comment: Calendar date of the heartbeat.
    format:
      type: date
      date_format: year_month_day
  - name: Account ID
    expr: source.account_id
    comment: Account the failover group belongs to.
  - name: Failover Group
    expr: source.failover_group_name
    comment: Fully qualified name of the failover group.
  - name: Replication State
    expr: source.replication_state
    comment: Replication state at heartbeat emission time.
  - name: Effective Primary Region
    expr: source.effective_primary_region
    comment: Primary region at heartbeat emission time.
measures:
  - name: Heartbeat Events
    expr: COUNT(1)
    comment: Number of replication heartbeat events in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Failover Groups
    expr: COUNT(DISTINCT source.failover_group_name)
    comment: Distinct number of failover groups in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Avg Replication Lag (ms)
    expr: AVG(source.replication_lag_ms)
    comment: Average milliseconds since the last successful replication.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Max Replication Lag (ms)
    expr: MAX(source.replication_lag_ms)
    comment: Maximum milliseconds since the last successful replication.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

# ============================================================
# clean_room_events_metrics  (system.access.clean_room_events)
# ============================================================
