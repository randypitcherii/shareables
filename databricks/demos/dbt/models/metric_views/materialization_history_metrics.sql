{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/misc_workspace_facts.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1
source: {{ source('system_sharing', 'materialization_history') }}
comment: Delta Sharing materialization events with workspace context.
joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true
dimensions:
  - name: Created At
    expr: source.created_at
    comment: Timestamp of when the materialization was created.
  - name: Created Date
    expr: CAST(source.created_at AS DATE)
    comment: Calendar date the materialization was created.
    format:
      type: date
      date_format: year_month_day
  - name: Workspace ID
    expr: source.workspace_id
    comment: The ID of the Databricks workspace where the materialization was billed to.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Recipient Name
    expr: source.recipient_name
    comment: The name of the downstream recipient using the data materialization.
  - name: Provider Name
    expr: source.provider_name
    comment: The name of the upstream provider using the data materialization.
  - name: Share Name
    expr: source.share_name
    comment: Name of share used to create materialization.
  - name: Schema Name
    expr: source.schema_name
    comment: Name of the schema of the shared asset used to create materialization.
  - name: Table Name
    expr: source.table_name
    comment: Name of the table used to create materialization.
measures:
  - name: Materialization Events
    expr: COUNT(1)
    comment: Number of materialization events in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Recipients
    expr: COUNT(DISTINCT source.recipient_name)
    comment: Distinct number of downstream recipients.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Shares
    expr: COUNT(DISTINCT source.share_name)
    comment: Distinct number of shares.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

# ============================================================
# predictive_optimization_metrics
# ============================================================
