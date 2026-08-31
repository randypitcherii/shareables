{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/misc_workspace_facts.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1
source: {{ source('system_lakeflow', 'zerobus_stream') }}
comment: Zerobus stream events with workspace context (Beta).
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
    comment: Time of log event.
  - name: Event Date
    expr: CAST(source.event_time AS DATE)
    comment: Calendar date of the event.
    format:
      type: date
      date_format: year_month_day
  - name: Workspace ID
    expr: source.workspace_id
    comment: ID of the workspace that the stream is created in.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Stream ID
    expr: source.stream_id
    comment: ID of the created stream.
  - name: Producer ID
    expr: source.producer_id
    comment: Custom ID of the client producer application used to ingest data.
  - name: Table Name
    expr: source.table_name
    comment: Fully qualified table name in human readable format.
  - name: Protocol
    expr: source.protocol
    comment: Protocol used for data ingestion.
  - name: Data Format
    expr: source.data_format
    comment: Data format used for passing data.
measures:
  - name: Stream Events
    expr: COUNT(1)
    comment: Number of stream events in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Streams
    expr: COUNT(DISTINCT source.stream_id)
    comment: Distinct number of streams in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Producers
    expr: COUNT(DISTINCT source.producer_id)
    comment: Distinct number of producer applications in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
