{{ config(materialized='metric_view', tags=['r23_metric_view']) }}
version: 1.1
source: {{ ref('r10_incr_append') }}
dimensions:
  - name: Event Date
    expr: event_date
measures:
  - name: Total Amount
    expr: SUM(amount)
