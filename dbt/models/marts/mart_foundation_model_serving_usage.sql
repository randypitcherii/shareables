with model_serving_usage as (
    select * from {{ ref('fct_model_serving_usage') }}
),
latest_list_prices as (
    select * from {{ ref('dim_latest_list_prices') }}
),
latest_model_serving_endpoints as (
    select * from {{ ref('dim_latest_model_serving_endpoints') }}
)
select
  usage.workspace_id,
  usage.sku_name,
  usage.usage_quantity,
  usage.usage_date,
  usage.billing_origin_product,
  usage.usage_metadata,
  usage.custom_tags,
  usage.identity_metadata,
  usage.usage_unit,
  usage.usage_start_time,
  usage.usage_end_time,
  usage.usage_quantity * pricing.list_price_usd as usage_list_price_usd,
  endpoints.endpoint_name,
  endpoints.endpoint_created_by,
  endpoints.is_deleted as endpoint_is_deleted
from model_serving_usage usage 
left outer join latest_list_prices pricing 
  on usage.sku_name = pricing.sku_name
left outer join latest_model_serving_endpoints endpoints
  on usage.usage_metadata.endpoint_id = endpoints.endpoint_id 