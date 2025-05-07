with source as (
    select * from {{ ref('stg_system_billing_list_prices') }} -- UPDATED REF
)
select
    sku_name,
    currency_code,
    pricing,
    pricing.default::float as list_price_usd, -- Adjusted based on original query
    price_start_time,
    price_end_time
from source
where price_end_time is null 