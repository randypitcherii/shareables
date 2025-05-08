-- mart_account_usage_analyze_tags_overtime.sql
-- Prepares data for analyzing tag-based usage over time.
-- Original query did complex HTML pivots. This model provides grouped data for BI tools.
-- It uses the same simplified tag string as mart_account_usage_analyze_tags.

with usage_with_tag_string as (
    select
        usage_usd,
        usage_date,
        usage_week,
        usage_month,
        case 
            when size(custom_tags) > 0 then
                array_join(
                    sort_array(
                        transform(
                            map_entries(custom_tags),
                            kv -> concat(kv.key, '=', kv.value)
                        )
                    ),
                    ';'
                )
            else '<NO_TAGS>'
        end as custom_tag_key_value_pairs_string
    from {{ ref('fct_usage_with_workspace_and_pricing') }}
),

-- For this example, let's demonstrate PoP for tags weekly.
grouped_usage_by_period as (
    select
        usage_week as period_key, -- Using week as the example period
        custom_tag_key_value_pairs_string as group_key,
        sum(usage_usd) as usage_usd_sum
    from usage_with_tag_string
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
        'ALL_TAGS_TOTAL' as group_key, -- Literal to signify total for tags
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