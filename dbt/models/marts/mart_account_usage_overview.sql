-- mart_account_usage_overview.sql
-- This model provides an overview of usage, allowing grouping by different time granularities and keys.
-- Parameters for time_key and group_key from the original dashboard query are handled by selecting 
-- all relevant columns and allowing downstream tools to perform the dynamic selection/grouping.

with base_usage as (
    select 
        usage_usd,
        usage_date,    -- for day
        usage_week,    -- for week
        usage_month,   -- for month
        -- usage_quarter, -- available if needed
        workspace_display_name,
        sku_name,
        billing_origin_product
    from {{ ref('fct_usage_with_workspace_and_pricing') }}
    -- The original query had :param_workspace here. 
    -- Filtering by workspace_display_name can be done in the BI tool or by adding a dbt variable if needed.
),

-- The original query had logic to normalize workspace_display_name if count > 50 or if it was in top 10.
-- This kind of top-N / normalization is often better handled in the BI layer or a subsequent mart specific to that view.
-- For this mart, we provide the detailed data.

-- The original query also used IDENTIFIER(:param_time_key) and IDENTIFIER(:param_group_key)
-- In dbt, it's generally better to output all potential grouping columns and let the BI tool handle pivoting/dynamic grouping,
-- or create separate marts for very specific views if performance of dynamic grouping is an issue.

final_select as (
    select
        -- Time keys
        usage_date as time_key_day,
        usage_week as time_key_week,
        usage_month as time_key_month,
        
        -- Group keys
        workspace_display_name as group_key_workspace,
        sku_name as group_key_sku,
        billing_origin_product as group_key_billing_origin_product,
        
        -- Metric
        usage_usd
    from base_usage
)

select * from final_select 