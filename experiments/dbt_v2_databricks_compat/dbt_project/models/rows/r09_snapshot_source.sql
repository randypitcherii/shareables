{#- On batch 2 one customer moves region, so the snapshot must close a row. -#}
{{ config(materialized='view', tags=['r09_snapshot']) }}
select customer_id, customer_name,
  case when customer_id = 1 and {{ var('batch') }} >= 2 then 'west' else region end as region
from {{ ref('customers') }}
