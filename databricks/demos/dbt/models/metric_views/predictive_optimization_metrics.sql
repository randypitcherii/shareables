{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/misc_workspace_facts.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1
source: {{ source('system_storage', 'predictive_optimization_operations_history') }}
comment: Predictive optimization operations with workspace context.
joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true
dimensions:
  - name: Start Time
    expr: source.start_time
    comment: The time at which the operation started. Recorded in UTC.
  - name: Start Date
    expr: CAST(source.start_time AS DATE)
    comment: Calendar date the operation started.
    format:
      type: date
      date_format: year_month_day
  - name: Workspace ID
    expr: source.workspace_id
    comment: The ID of the workspace in which predictive optimization ran the operation.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Operation Type
    expr: source.operation_type
    comment: The optimization operation which was performed.
  - name: Operation Status
    expr: source.operation_status
    comment: The status of the optimization operation (SUCCESSFUL or FAILED with a specific error type).
  - name: Metastore Name
    expr: source.metastore_name
    comment: The name of the metastore to which the optimized table belongs.
  - name: Catalog Name
    expr: source.catalog_name
    comment: The name of the catalog to which the optimized table belongs.
  - name: Schema Name
    expr: source.schema_name
    comment: The name of the schema to which the optimized table belongs.
  - name: Table Name
    expr: source.table_name
    comment: The name of the optimized table.
measures:
  - name: Operations
    expr: COUNT(1)
    comment: Number of predictive optimization operations in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Successful Operations
    expr: COUNT(1) FILTER (WHERE source.operation_status = 'SUCCESSFUL')
    comment: Number of operations that completed successfully.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total Usage Quantity
    expr: SUM(source.usage_quantity)
    comment: Total amount of the usage unit consumed by these operations.
    format:
      type: number
      decimal_places:
        type: exact
        places: 2
  - name: Distinct Tables
    expr: COUNT(DISTINCT source.table_id)
    comment: Distinct number of optimized tables in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

# ============================================================
# zerobus_ingest_metrics
# ============================================================
