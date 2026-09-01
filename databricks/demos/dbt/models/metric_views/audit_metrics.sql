{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/access_events.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1
source: {{ source('system_access', 'audit') }}
comment: Databricks audit events with workspace context. Account-level events have workspace_id 0.
joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true
dimensions:
  - name: Event Time
    expr: source.event_time
    comment: Timestamp of the event.
  - name: Event Date
    expr: source.event_date
    comment: Calendar date of the event.
    format:
      type: date
      date_format: year_month_day
  - name: Workspace ID
    expr: source.workspace_id
    comment: The Databricks workspace in which the audit event took place. Account-level events have workspace_id 0.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Service Name
    expr: source.service_name
    comment: The name of the Databricks service that generated the audit event.
  - name: Action Name
    expr: source.action_name
    comment: The name of the action performed as part of the audit event.
  - name: Audit Level
    expr: source.audit_level
    comment: Whether the event is at the workspace or account level (ACCOUNT_LEVEL or WORKSPACE_LEVEL).
measures:
  - name: Audit Events
    expr: COUNT(1)
    comment: Number of audit events in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Sessions
    expr: COUNT(DISTINCT source.session_id)
    comment: Distinct number of sessions in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Actions
    expr: COUNT(DISTINCT source.action_name)
    comment: Distinct number of action types in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

# ============================================================
# assistant_events_metrics  (source: system.access.assistant_events)
# ============================================================
