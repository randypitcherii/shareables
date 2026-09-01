{#-
  End-to-end guard on the metric views' core relational assumption: every join
  is N:1, so joining dimensions must never change the fact grain.

  billing_usage_metrics carries the project's heaviest join graph (workspace,
  point-in-time price, cluster/job/pipeline SCD windows, serving endpoints). If
  ANY of those joins starts matching more than one row -- overlapping price
  windows, duplicated dimension rows, a broken SCD validity window -- the view
  quietly inflates counts and cost. This test recomputes the raw fact row count
  for the last 7 days and fails unless MEASURE(`Usage Records`) over the same
  slice is identical.

  Metric views only answer aggregate questions, so the comparison goes through
  MEASURE() rather than a row-level diff.
-#}
with expected as (
    select count(*) as n_usage_records
    from {{ source('system_billing', 'usage') }}
    where usage_date >= current_date() - interval 7 days
),

actual as (
    select measure(`Usage Records`) as n_usage_records
    from {{ ref('billing_usage_metrics') }}
    where `Usage Date` >= current_date() - interval 7 days
)

select
    expected.n_usage_records as expected_records,
    actual.n_usage_records   as metric_view_records
from expected
cross join actual
where expected.n_usage_records != actual.n_usage_records
