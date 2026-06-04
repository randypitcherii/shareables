{#-
  Guards the price join. DBU usage should almost always resolve to a list price, so a
  large share of unpriced DBU records means the join (sku_name + cloud + effective-date
  window) is broken. Fails if more than 5% of the last 7 days of positive DBU usage
  records have no matched price.
-#}
with recent as (
    select list_cost
    from {{ ref('int_usage_priced') }}
    where usage_date >= current_date() - interval 7 days
      and usage_unit = 'DBU'
      and usage_quantity > 0
),

summary as (
    select
        count(*)                                           as n_records,
        sum(case when list_cost is null then 1 else 0 end) as n_unpriced
    from recent
)

select *
from summary
where n_records > 0
  and (n_unpriced * 1.0 / n_records) > 0.05
