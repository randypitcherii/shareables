{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/job_run_timeline.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1

# Fact: system.lakeflow.job_run_timeline (one job-run time period).
# Grain (workspace_id, run_id, period_start_time). Inline SCD job join,
# point-in-time on period_start_time. Validated N:1 (0 many-to-many).
source: {{ source('system_lakeflow', 'job_run_timeline') }}

comment: Databricks job run executions with point-in-time job definition and workspace context.

joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  - name: job
    source: |
      SELECT workspace_id, job_id, name AS job_name, creator_id, run_as, description, change_time,
             lead(change_time) OVER (PARTITION BY workspace_id, job_id ORDER BY change_time) AS next_change_time
      FROM {{ source('system_lakeflow', 'jobs') }}
    "on": job.workspace_id = source.workspace_id
      AND job.job_id = source.job_id
      AND source.period_start_time >= job.change_time
      AND (source.period_start_time < job.next_change_time OR job.next_change_time IS NULL)
    cardinality: many_to_one
    rely:
      at_most_one_match: true

dimensions:
  - name: Period Start
    expr: source.period_start_time
    comment: The start time for the run or for the time period. Recorded in UTC.
  - name: Period Date
    expr: CAST(source.period_start_time AS DATE)
    comment: Calendar date of the run period.
    format:
      type: date
      date_format: year_month_day
  - name: Period Month
    expr: DATE_TRUNC('MONTH', source.period_start_time)
    comment: Month bucket of the run period for monthly trend analysis.
    format:
      type: date
      date_format: locale_short_month
  - name: Workspace ID
    expr: source.workspace_id
    comment: The ID of the workspace this job belongs to.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Job ID
    expr: source.job_id
    comment: The ID of the job. This key is only unique within a single workspace.
  - name: Job Name
    expr: job.job_name
    comment: The user-supplied name of the job.
  - name: Job Creator
    expr: job.creator_id
    comment: The ID of the principal who created the job.
  - name: Job Run As
    expr: job.run_as
    comment: The ID of the user or service principal whose permissions are used for the job run.
  - name: Result State
    expr: source.result_state
    comment: The outcome of the job run.
  - name: Trigger Type
    expr: source.trigger_type
    comment: The type of trigger that can fire a run.
  - name: Run Type
    expr: source.run_type
    comment: The type of job run.
  - name: Termination Code
    expr: source.termination_code
    comment: The termination code of the job run. Not populated for rows emitted before late August 2024.
  - name: Termination Type
    expr: source.termination_type
    comment: The type of termination for the job run. Not populated for rows emitted before late November 2025.

measures:
  - name: Run Periods
    expr: COUNT(1)
    comment: Number of run time-period rows in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Runs
    expr: COUNT(DISTINCT source.run_id)
    comment: Distinct number of job runs in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Jobs
    expr: COUNT(DISTINCT source.job_id)
    comment: Distinct number of jobs in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Success Runs
    expr: COUNT(DISTINCT source.run_id) FILTER (WHERE source.result_state = 'SUCCEEDED')
    comment: Distinct runs that completed successfully.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Failed Runs
    expr: COUNT(DISTINCT source.run_id) FILTER (WHERE source.result_state = 'FAILED')
    comment: Distinct runs that failed.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Avg Run Duration (s)
    expr: AVG(source.run_duration_seconds)
    comment: Average total run duration in seconds. Not populated for rows before late November 2025.
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: Total Run Duration (s)
    expr: SUM(source.run_duration_seconds)
    comment: Total run duration in seconds across the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Avg Queue Duration (s)
    expr: AVG(source.queue_duration_seconds)
    comment: Average time spent queued in seconds. Not populated for rows before late November 2025.
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
