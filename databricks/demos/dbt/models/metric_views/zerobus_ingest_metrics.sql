{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/misc_workspace_facts.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1
source: {{ source('system_lakeflow', 'zerobus_ingest') }}
comment: Zerobus ingestion commit records with workspace context (Beta).
joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true
dimensions:
  - name: Commit Time
    expr: source.commit_time
    comment: Time of the commit.
  - name: Commit Date
    expr: CAST(source.commit_time AS DATE)
    comment: Calendar date of the commit.
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
    comment: ID of the stream that performed the ingestion.
  - name: Table Name
    expr: source.table_name
    comment: Fully qualified table name in human readable format.
measures:
  - name: Commits
    expr: COUNT(1)
    comment: Number of ingestion commits in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total Committed Bytes
    expr: SUM(source.committed_bytes)
    comment: Total size of ingested data in bytes.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total Committed Records
    expr: SUM(source.committed_records)
    comment: Total number of committed records.
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

# ============================================================
# zerobus_stream_metrics
# ============================================================
