{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/access_events.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1
source: {{ source('system_access', 'outbound_network') }}
comment: Denied outbound network access events with workspace context.
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
  - name: Destination Type
    expr: source.destination_type
    comment: The type of destination. Possible values are DNS, IP, and STORAGE.
  - name: Destination
    expr: source.destination
    comment: Details of the destination (domain name, IP address, or storage location).
  - name: Access Type
    expr: source.access_type
    comment: Type of access event that occurred.
  - name: Network Source Type
    expr: source.network_source_type
    comment: The source type of the network event, mainly for customer debugging.
measures:
  - name: Outbound Denied Events
    expr: COUNT(1)
    comment: Number of denied outbound access events in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Destinations
    expr: COUNT(DISTINCT source.destination)
    comment: Distinct number of destinations in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
