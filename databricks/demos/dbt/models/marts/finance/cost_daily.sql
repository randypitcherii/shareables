{#- Daily list cost and usage, by cloud. The headline cost-monitoring rollup. -#}
select
    usage_date,
    cloud,
    currency_code,
    count(*)            as n_usage_records,
    sum(usage_quantity) as usage_quantity,
    sum(list_cost)      as list_cost
from {{ ref('int_usage_priced') }}
group by usage_date, cloud, currency_code
