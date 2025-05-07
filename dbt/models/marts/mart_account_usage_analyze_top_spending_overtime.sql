-- mart_account_usage_analyze_top_spending_overtime.sql
-- Prepares data for analyzing top spending usage over time.
-- Original query did complex HTML pivots. This model provides grouped data for BI tools.
-- It provides several common rank keys; the BI tool would select one for PoP display.

with base_usage as (
    select 
        usage_usd,
        usage_date,
        usage_week,
        usage_month,
        workspace_display_name,
        identity_metadata.run_as as run_as_user, 
        usage_metadata.job_id as job_id,
        usage_metadata.notebook_id as notebook_id,
        sku_name,
        billing_origin_product
    from {{ ref('fct_usage_with_workspace_and_pricing') }}
),

-- For this example, let's demonstrate PoP for 'billing_origin_product' weekly.
-- The BI tool would dynamically select the group_key and period_key.
grouped_usage_by_period as (
    select
        usage_week as period_key, -- Example period
        billing_origin_product as group_key, -- Example group key
        sum(usage_usd) as usage_usd_sum
    from base_usage
    group by 1, 2
),

grouped_usage_change as (
    select
        period_key,
        group_key,
        usage_usd_sum,
        lag(usage_usd_sum, 1) over (partition by group_key order by period_key) as prev_period_usage_usd_sum,
        round({{ dbt_utils.safe_divide('usage_usd_sum - lag(usage_usd_sum, 1) over (partition by group_key order by period_key)', 'lag(usage_usd_sum, 1) over (partition by group_key order by period_key)') }} * 100, 2) as usage_change_percentage
    from grouped_usage_by_period
),

total_usage_by_period as (
    select
        period_key,
        'ALL_RANK_KEYS_TOTAL' as group_key, -- Literal for total over the chosen rank key
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