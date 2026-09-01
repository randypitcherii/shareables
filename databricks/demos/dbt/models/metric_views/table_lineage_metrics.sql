{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/table_lineage.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1

# Fact: system.access.table_lineage (one table read/write lineage event).
# Joins to query.history AS A DIMENSION via the documented statement_id FK
# (query.history is one row per statement_id -> N:1). Validated N:1.
source: {{ source('system_access', 'table_lineage') }}

comment: Unity Catalog table lineage events with workspace and originating-query context.

joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  - name: query
    source: |
      SELECT workspace_id, statement_id, statement_type, execution_status, executed_by, start_time AS query_start_time
      FROM {{ source('system_query', 'history') }}
    "on": query.workspace_id = source.workspace_id
      AND query.statement_id = source.statement_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true

dimensions:
  - name: Event Time
    expr: source.event_time
    comment: The timestamp when the lineage was generated. Recorded in UTC.
  - name: Event Date
    expr: source.event_date
    comment: Calendar date the lineage was generated.
    format:
      type: date
      date_format: year_month_day
  - name: Workspace ID
    expr: source.workspace_id
    comment: The id of the workspace.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Entity Type
    expr: source.entity_type
    comment: The type of entity the lineage was captured from (NOTEBOOK, JOB, PIPELINE, DASHBOARD_V3, DBSQL_QUERY, ...).
  - name: Source Table
    expr: source.source_table_full_name
    comment: Three-part name to identify the source table.
  - name: Target Table
    expr: source.target_table_full_name
    comment: Three-part name to identify the target table.
  - name: Source Type
    expr: source.source_type
    comment: The type of the source (TABLE, PATH, VIEW, MATERIALIZED_VIEW, METRIC_VIEW, STREAMING_TABLE).
  - name: Target Type
    expr: source.target_type
    comment: The type of the target (TABLE, PATH, VIEW, MATERIALIZED_VIEW, METRIC_VIEW, STREAMING_TABLE).
  - name: Created By
    expr: source.created_by
    comment: The user who generated this lineage (username, service principal ID, group name, System-User, or NULL).
  - name: Query Statement Type
    expr: query.statement_type
    comment: 'Statement type of the originating query (from query.history).'
  - name: Query Execution Status
    expr: query.execution_status
    comment: Execution status of the originating query (from query.history).
  - name: Query Executed By
    expr: query.executed_by
    comment: User who ran the originating query (from query.history).

measures:
  - name: Lineage Events
    expr: COUNT(1)
    comment: Number of table lineage events in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Source Tables
    expr: COUNT(DISTINCT source.source_table_full_name)
    comment: Distinct number of source tables in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Target Tables
    expr: COUNT(DISTINCT source.target_table_full_name)
    comment: Distinct number of target tables in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Statements
    expr: COUNT(DISTINCT source.statement_id)
    comment: Distinct number of originating query statements.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
