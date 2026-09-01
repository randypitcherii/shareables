{{ config(materialized='metric_view') }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/ai_gateway.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.

version: 1.1
source: {{ source('system_ai_gateway', 'external_model_spend') }}
comment: AI Gateway external model estimated spend with workspace context.
joins:
  - name: ws
    source: {{ source('system_access', 'workspaces_latest') }}
    "on": source.workspace_id = ws.workspace_id
    cardinality: many_to_one
    rely:
      at_most_one_match: true
dimensions:
  - name: Usage Date
    expr: source.usage_date
    comment: Date of the usage record (derived from usage_start_time).
    format:
      type: date
      date_format: year_month_day
  - name: Usage Start Time
    expr: source.usage_start_time
    comment: Start of the hourly aggregation window in UTC.
  - name: Workspace ID
    expr: source.workspace_id
    comment: The ID of the Databricks workspace where the AI Gateway endpoint is configured.
  - name: Workspace Name
    expr: ws.workspace_name
    comment: The human-readable name of the workspace.
  - name: Usage Unit
    expr: source.usage_unit
    comment: Unit of measurement for usage_quantity (always USD).
measures:
  - name: Spend Records
    expr: COUNT(1)
    comment: Number of aggregated spend records in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total Spend (USD)
    expr: SUM(source.usage_quantity)
    comment: Total estimated cost in USD across the slice.
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
