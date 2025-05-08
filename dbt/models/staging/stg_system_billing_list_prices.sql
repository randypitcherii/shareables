select
    sku_name,
    currency_code,
    pricing,
    price_start_time,
    price_end_time
from {{ source('system_billing', 'list_prices') }} 