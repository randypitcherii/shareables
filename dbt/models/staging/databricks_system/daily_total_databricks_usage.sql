select  
  usage_start_time::date as usage_day,
  sum(usage_quantity) as total_amount

from {{ source('databricks_billing', 'usage') }}

group by all