{#-
  Discriminating correctness check.

  A green `dbt build` can STILL emit all-NULL or zero costs if the price join is
  broken. This test fails the run if a recent, settled day has no positive list cost,
  catching exactly that class of silent failure.

  We look two days back from the latest date so a partially-ingested current day
  doesn't trip the test.
-#}
with daily as (
    select usage_date, sum(list_cost) as list_cost
    from {{ ref('cost_daily') }}
    group by usage_date
),

settled_day as (
    select max(usage_date) - interval 2 days as as_of from daily
)

select daily.*
from daily
cross join settled_day
where daily.usage_date = settled_day.as_of
  and (daily.list_cost is null or daily.list_cost <= 0)
