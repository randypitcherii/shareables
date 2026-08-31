{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/run_metrics_history.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1

# Fact: system.mlflow.run_metrics_history (one logged metric datapoint).
# Snapshot joins to runs_latest and experiments_latest (both unique by their PK).
# Validated N:1.
source: {{ source('system_mlflow', 'run_metrics_history') }}

comment: MLflow logged metric datapoints with run and experiment context.

joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  - name: run
    source: |
      SELECT workspace_id, run_id, run_name, status, created_by, start_time, end_time
      FROM {{ source('system_mlflow', 'runs_latest') }}
    "on": run.workspace_id = source.workspace_id AND run.run_id = source.run_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true

  - name: experiment
    source: |
      SELECT workspace_id, experiment_id, name AS experiment_name
      FROM {{ source('system_mlflow', 'experiments_latest') }}
    "on": experiment.workspace_id = source.workspace_id AND experiment.experiment_id = source.experiment_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true

dimensions:
  - name: Metric Time
    expr: source.metric_time
    comment: The user-specified time when the metric was computed.
  - name: Metric Date
    expr: CAST(source.metric_time AS DATE)
    comment: Calendar date the metric was computed.
    format:
      type: date
      date_format: year_month_day
  - name: Workspace ID
    expr: source.workspace_id
    comment: The ID of the workspace containing the MLflow run to which the metric was logged.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Metric Name
    expr: source.metric_name
    comment: The metric name.
  - name: Experiment ID
    expr: source.experiment_id
    comment: The ID of the MLflow experiment containing the run to which the metric was logged.
  - name: Experiment Name
    expr: experiment.experiment_name
    comment: User-provided name of the experiment.
  - name: Run ID
    expr: source.run_id
    comment: The ID of the MLflow run to which the metric was logged.
  - name: Run Name
    expr: run.run_name
    comment: The name of the MLflow run.
  - name: Run Status
    expr: run.status
    comment: The execution status of the MLflow run.
  - name: Run Created By
    expr: run.created_by
    comment: The Databricks principal, user, or group that created the MLflow run.

measures:
  - name: Metric Datapoints
    expr: COUNT(1)
    comment: Number of logged metric datapoints in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Runs
    expr: COUNT(DISTINCT source.run_id)
    comment: Distinct number of MLflow runs in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Experiments
    expr: COUNT(DISTINCT source.experiment_id)
    comment: Distinct number of MLflow experiments in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Metrics
    expr: COUNT(DISTINCT source.metric_name)
    comment: Distinct number of metric names in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Avg Metric Value
    expr: AVG(source.metric_value)
    comment: Average of the logged metric values (across all metric names in the slice).
    format:
      type: number
      decimal_places:
        type: exact
        places: 4
