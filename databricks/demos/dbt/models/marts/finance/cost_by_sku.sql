{#- Daily list cost by SKU + product -- the finest-grained cost breakdown. -#}
select
    sku_name,
    billing_origin_product,
    usage_unit,
    usage_date,
    cloud,
    currency_code,
    sum(usage_quantity) as usage_quantity,
    sum(list_cost)      as list_cost
from {{ ref('int_usage_priced') }}
group by sku_name, billing_origin_product, usage_unit, usage_date, cloud, currency_code
