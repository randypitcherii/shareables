{{ config(materialized='metric_view', enabled=false) }}
# Ported from HobbsAnalytics/databricks-metric-views-system-tables
# (metric_views/standalone_facts.yaml). system.* references are rewritten to dbt
# source() calls for lineage; the metric-view YAML is otherwise unchanged.
# DISABLED: upstream marks this view blocked pending a SELECT grant on its
# source system table. Flip enabled once the grant exists in this account.

version: 1.1
source: {{ source('system_data_quality_monitoring', 'table_results') }}
comment: Data quality monitoring results at table grain (standalone fact).
dimensions:
  - name: Event Time
    expr: source.event_time
    comment: Time of the data quality monitoring evaluation.
  - name: Event Date
    expr: CAST(source.event_time AS DATE)
    comment: Calendar date of the evaluation.
    format:
      type: date
      date_format: year_month_day
  - name: Catalog Name
    expr: source.catalog_name
    comment: Catalog of the monitored table.
  - name: Schema Name
    expr: source.schema_name
    comment: Schema of the monitored table.
  - name: Table Name
    expr: source.table_name
    comment: Name of the monitored table.
  - name: Status
    expr: source.status
    comment: Overall monitoring status for the table.
  - name: Freshness Status
    expr: source.freshness.status
    comment: Freshness check status for the table.
  - name: Completeness Status
    expr: source.completeness.status
    comment: Completeness check status for the table.
measures:
  - name: Results
    expr: COUNT(1)
    comment: Number of monitoring result rows in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Distinct Tables
    expr: COUNT(DISTINCT source.table_id)
    comment: Distinct number of monitored tables in the slice.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Unhealthy Results
    expr: COUNT(1) FILTER (WHERE source.status <> 'HEALTHY')
    comment: Number of results whose status is not HEALTHY.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: Total Downstream Tables
    expr: SUM(source.downstream_impact.num_downstream_tables)
    comment: Sum of downstream tables potentially impacted.
    format:
      type: number
      decimal_places:
        type: exact
        places: 0

# ============================================================
# replication_states_metrics  (system.replication.states)
# ============================================================
