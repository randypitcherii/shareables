{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/access_events.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1
source: {{ source('system_access', 'assistant_events') }}
comment: Genie/assistant message events with workspace context.
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
    comment: Time that the event happened. Recorded in UTC.
  - name: Event Date
    expr: source.event_date
    comment: Calendar date of the event.
    format:
      type: date
      date_format: year_month_day
  - name: Workspace ID
    expr: source.workspace_id
    comment: ID of the workspace.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: User Agent
    expr: source.user_agent
    comment: Origination of request.
  - name: Initiated By
    expr: source.initiated_by
    comment: Email of the user initiating the request.
measures:
  - name: Assistant Events
    expr: COUNT(1)
    comment: Number of assistant/Genie message events in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Users
    expr: COUNT(DISTINCT source.initiated_by)
    comment: Distinct number of users initiating requests.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

# ============================================================
# inbound_network_metrics  (source: system.access.inbound_network)
# ============================================================
