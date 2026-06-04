{#- Daily list cost by Databricks product (Jobs, SQL, Model Serving, DLT, ...). -#}
select
    billing_origin_product,
    usage_date,
    cloud,
    count(distinct workspace_id) as n_workspaces,
    sum(usage_quantity)          as usage_quantity,
    sum(list_cost)               as list_cost
from {{ ref('int_usage_priced') }}
group by billing_origin_product, usage_date, cloud
