{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/pipeline_update_timeline.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1

# Fact: system.lakeflow.pipeline_update_timeline (one pipeline update time period).
# Grain (workspace_id, update_id, period_start_time). Inline SCD pipeline join,
# point-in-time on period_start_time. Validated N:1.
source: {{ source('system_lakeflow', 'pipeline_update_timeline') }}

comment: Databricks pipeline (DLT) update executions with point-in-time pipeline definition context.

joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  - name: pipeline
    source: |
      SELECT workspace_id, pipeline_id, name AS pipeline_name, pipeline_type, created_by, run_as, change_time,
             lead(change_time) OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time) AS next_change_time
      FROM {{ source('system_lakeflow', 'pipelines') }}
    "on": pipeline.workspace_id = source.workspace_id
      AND pipeline.pipeline_id = source.pipeline_id
      AND source.period_start_time >= pipeline.change_time
      AND (source.period_start_time < pipeline.next_change_time OR pipeline.next_change_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

dimensions:
  - name: Period Start
    expr: source.period_start_time
    comment: The start time for the pipeline update or for the hour. Stored as a UTC timestamp.
  - name: Period Date
    expr: CAST(source.period_start_time AS DATE)
    comment: Calendar date of the update period.
    format:
      type: date
      date_format: year_month_day
  - name: Period Month
    expr: DATE_TRUNC('MONTH', source.period_start_time)
    comment: Month bucket of the update period for monthly trend analysis.
    format:
      type: date
      date_format: locale_short_month
  - name: Workspace ID
    expr: source.workspace_id
    comment: The ID of the workspace this pipeline belongs to.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Pipeline ID
    expr: source.pipeline_id
    comment: The ID of the pipeline. Unique within a workspace.
  - name: Pipeline Name
    expr: pipeline.pipeline_name
    comment: The user-supplied name of the pipeline.
  - name: Pipeline Type
    expr: pipeline.pipeline_type
    comment: The type of pipeline.
  - name: Update Type
    expr: source.update_type
    comment: The type of the pipeline update.
  - name: Trigger Type
    expr: source.trigger_type
    comment: What triggered this update.
  - name: Result State
    expr: source.result_state
    comment: The outcome of the pipeline update.
  - name: Run As
    expr: source.run_as_user_name
    comment: The email/ID of the service principal or group whose permissions are used for the update.

measures:
  - name: Update Periods
    expr: COUNT(1)
    comment: Number of update time-period rows in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Updates
    expr: COUNT(DISTINCT source.update_id)
    comment: Distinct number of pipeline updates in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Pipelines
    expr: COUNT(DISTINCT source.pipeline_id)
    comment: Distinct number of pipelines in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Completed Updates
    expr: COUNT(DISTINCT source.update_id) FILTER (WHERE source.result_state = 'COMPLETED')
    comment: Distinct updates that completed.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Failed Updates
    expr: COUNT(DISTINCT source.update_id) FILTER (WHERE source.result_state = 'FAILED')
    comment: Distinct updates that failed.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
