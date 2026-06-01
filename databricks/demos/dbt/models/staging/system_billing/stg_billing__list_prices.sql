with source as (
    select * from {{ source('system_billing', 'list_prices') }}
)

select
    account_id,
    sku_name,
    cloud,
    currency_code,
    usage_unit,

    -- effective-dated price window
    price_start_time,
    price_end_time,

    -- pricing.default is the simple published list rate.
    -- pricing.effective_list.default resolves list + promotional pricing and is the
    -- value Databricks documents for CALCULATING cost -> use this one downstream.
    pricing.default                as list_price,
    pricing.effective_list.default as effective_list_price

from source
