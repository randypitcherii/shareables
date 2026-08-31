{{ config(materialized='metric_view', enabled=false) }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/standalone_facts.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.
# DISABLED: SELECT on this source table is denied even with catalog-wide SELECT
# on `system` -- this schema needs its own explicit grant (verified 2026-08-31).
# Flip enabled once an account admin grants SELECT on the source table.

version: 1.1
source: {{ source('system_data_classification', 'results') }}
comment: Data classification detections (current-state snapshot standalone fact).
dimensions:
  - name: Latest Detected Time
    expr: source.latest_detected_time
    comment: Most recent time this classification was detected.
  - name: Latest Detected Date
    expr: CAST(source.latest_detected_time AS DATE)
    comment: Calendar date of the latest detection.
    format:
      type: date
      date_format: year_month_day
  - name: Catalog Name
    expr: source.catalog_name
    comment: Catalog of the classified table.
  - name: Schema Name
    expr: source.schema_name
    comment: Schema of the classified table.
  - name: Table Name
    expr: source.table_name
    comment: Name of the classified table.
  - name: Column Name
    expr: source.column_name
    comment: Name of the classified column.
  - name: Data Type
    expr: source.data_type
    comment: Data type of the classified column.
  - name: Class Tag
    expr: source.class_tag
    comment: The sensitive-data class tag detected.
  - name: Confidence
    expr: source.confidence
    comment: Confidence level of the detection.
  - name: Exclusion State
    expr: source.exclusion_state
    comment: Whether the detection has been excluded.
measures:
  - name: Detections
    expr: COUNT(1)
    comment: Number of column-level detections in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Tables
    expr: COUNT(DISTINCT source.table_id)
    comment: Distinct number of classified tables in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Class Tags
    expr: COUNT(DISTINCT source.class_tag)
    comment: Distinct number of sensitive-data class tags in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
