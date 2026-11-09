{{ config(materialized='table', tags=['r06_table']) }}
select c.region, sum(e.amount) as total_amount
from {{ ref('r05_view') }} e
join {{ ref('customers') }} c using (customer_id)
group by c.region
