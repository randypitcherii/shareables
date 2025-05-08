-- mart_account_usage_overview_overtime.sql
-- This model prepares data for analyzing usage over time, with groupings.
-- The original query performed complex pivoting and HTML formatting for display.
-- This dbt model will provide the underlying grouped data, and period-over-period calculations if straightforward.
-- Pivoting and HTML generation should be handled by the BI tool.

-- Parameters like :param_time_key, :param_group_key, :param_start_date, :param_end_date, :param_workspace 
-- are handled by selecting all relevant columns from the upstream fct_usage_with_workspace_and_pricing model.

with base_usage_with_time_keys as (
    select 
        usage_usd,
        usage_date,    -- for day
        usage_week,    -- for week
        usage_month,   -- for month
        workspace_display_name,
        sku_name,
        billing_origin_product
    from {{ ref('fct_usage_with_workspace_and_pricing') }}
),

-- Simplified: Choose a default time granularity (e.g., week) and group key (e.g., billing_origin_product) for PoP calculation.
-- For full dynamic selection as in the dashboard, the BI tool would need to do the aggregation and PoP on top of a more granular mart.
-- Or, create multiple marts for common PoP views.

-- For this example, let's demonstrate PoP for billing_origin_product weekly.

grouped_usage_by_period as (
    select
        usage_week as period_key, -- Using week as the example period
        billing_origin_product as group_key, -- Using billing_origin_product as the example group
        sum(usage_usd) as usage_usd_sum
    from base_usage_with_time_keys
    group by 1, 2
),

-- Add PoP calculation
grouped_usage_change as (
    select
        period_key,
        group_key,
        usage_usd_sum,
        lag(usage_usd_sum, 1) over (partition by group_key order by period_key) as prev_period_usage_usd_sum,
        round({{ dbt_utils.safe_divide('usage_usd_sum - lag(usage_usd_sum, 1) over (partition by group_key order by period_key)', 'lag(usage_usd_sum, 1) over (partition by group_key order by period_key)') }} * 100, 2) as usage_change_percentage
    from grouped_usage_by_period
),

-- Also calculate total PoP across all groups for the chosen period granularity
total_usage_by_period as (
    select
        period_key,
        'ALL_GROUPS_TOTAL' as group_key, -- Literal to signify total
        sum(usage_usd_sum) as usage_usd_sum
    from grouped_usage_by_period
    group by 1
),

total_usage_change as (
    select
        period_key,
        group_key,
        usage_usd_sum,
        lag(usage_usd_sum, 1) over (order by period_key) as prev_period_usage_usd_sum,
        round({{ dbt_utils.safe_divide('usage_usd_sum - lag(usage_usd_sum, 1) over (order by period_key)', 'lag(usage_usd_sum, 1) over (order by period_key)') }} * 100, 2) as usage_change_percentage
    from total_usage_by_period
)

select * from grouped_usage_change
union all
select * from total_usage_change
order by period_key, group_key 