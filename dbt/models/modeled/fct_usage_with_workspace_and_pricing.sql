-- This model prepares a base view of usage data joined with workspace names and list prices.
-- It replaces parameterized date filters with a direct selection from the source.
-- It also replaces the hardcoded workspace JSON with a reference to the seed table.

with usage_source as (
    select 
        workspace_id,
        sku_name,
        usage_quantity,
        usage_date,
        billing_origin_product,
        usage_metadata,
        custom_tags,
        identity_metadata,
        usage_unit,
        usage_start_time,
        usage_end_time
    from {{ ref('stg_system_billing_usage') }} -- UPDATED REF
    -- where usage_date between :param_start_date and :param_end_date -- Removed for dbt model
),

workspace_map as (
    select 
        workspace_id as map_workspace_id, -- aliasing to avoid potential conflict with usage.workspace_id
        workspace_name
    from {{ ref('databricks_workspace_map') }}
),

usage_with_ws as (
    select
        case
            when wm.workspace_name is null then concat('id: ', u.workspace_id)
            else concat(wm.workspace_name, ' (id: ', u.workspace_id, ')')
        end as workspace_display_name, -- This was 'workspace' in the original queries
        u.workspace_id,
        u.sku_name,
        u.usage_quantity,
        u.usage_date,
        u.billing_origin_product,
        u.usage_metadata,
        u.custom_tags,
        u.identity_metadata,
        u.usage_unit,
        u.usage_start_time,
        u.usage_end_time
    from usage_source u
    left join workspace_map wm
        on u.workspace_id = wm.map_workspace_id
),

prices as (
    select 
        sku_name as price_sku_name, 
        pricing.default::float as list_price_usd, 
        price_start_time,
        coalesce(price_end_time, date_add(current_date, 1)) as coalesced_price_end_time 
    from {{ ref('stg_system_billing_list_prices') }} -- UPDATED REF
    where currency_code = 'USD'
    and price_end_time is null 
),

list_priced_usd as (
    select
        coalesce(u.usage_quantity * p.list_price_usd, 0) as usage_usd,
        u.usage_date,
        date_trunc('QUARTER', u.usage_date) as usage_quarter,
        date_trunc('MONTH', u.usage_date) as usage_month,
        date_trunc('WEEK', u.usage_date) as usage_week,
        u.workspace_display_name,
        u.workspace_id,
        u.sku_name,
        u.usage_quantity,
        u.billing_origin_product,
        u.usage_metadata,
        u.custom_tags,
        u.identity_metadata,
        u.usage_unit,
        u.usage_start_time,
        u.usage_end_time
    from usage_with_ws u
    left join prices p
        on u.sku_name = p.price_sku_name
)

select * from list_priced_usd 