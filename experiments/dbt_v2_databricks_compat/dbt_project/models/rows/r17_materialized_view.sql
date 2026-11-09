{{ config(materialized='materialized_view', tags=['r17_materialized_view']) }}
select c.region, count(*) as orders, sum(o.order_amount) as revenue
from {{ ref('r16_streaming_table') }} o
join {{ ref('customers') }} c using (customer_id)
group by c.region
