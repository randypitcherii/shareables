{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/access_events.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1
source: {{ source('system_access', 'inbound_network') }}
comment: Denied inbound network access events with workspace context.
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
    comment: Timestamp when the event took place.
  - name: Event Date
    expr: CAST(source.event_time AS DATE)
    comment: Calendar date of the event.
    format:
      type: date
      date_format: year_month_day
  - name: Workspace ID
    expr: source.workspace_id
    comment: The ID of the workspace where the event occurred.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Request Path
    expr: source.request_path
    comment: The destination of the request.
  - name: Source IP
    expr: source.source.ip
    comment: Source IP address of the inbound request.
  - name: Policy Outcome
    expr: source.policy_outcome
    comment: Type of access event that occurred. DENIED or DRY_RUN_DENIAL.
  - name: Rule Label
    expr: source.rule_label
    comment: The label of the ingress rule that denied the request.
measures:
  - name: Inbound Denied Events
    expr: COUNT(1)
    comment: Number of denied inbound access events in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

# ============================================================
# outbound_network_metrics  (source: system.access.outbound_network)
# ============================================================
