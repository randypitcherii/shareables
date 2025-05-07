-- mart_account_usage_analyze_tags.sql
-- Analyzes usage grouped by custom tags.
-- The original query had complex parameter-driven tag parsing (tag_entries, single_tag_key, show_mismatch).
-- This dbt model simplifies to group by a string representation of all custom tags.

with base_usage as (
    select 
        usage_usd,
        custom_tags, -- map type
        usage_date,  -- for day
        usage_week,  -- for week
        usage_month  -- for month
    from {{ ref('fct_usage_with_workspace_and_pricing') }}
),

usage_with_tag_string as (
    select
        usage_usd,
        usage_date,
        usage_week,
        usage_month,
        -- Convert map to a sorted string to ensure consistent grouping. 
        -- Example: array_join(map_entries(custom_tags), '; ', ':') would create 'key1:val1; key2:val2'
        -- For a more robust solution, consider exploding tags into rows in a modeled table if specific tag keys are of interest.
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
    from base_usage
)

-- This mart provides data grouped by the tag string and time dimensions.
-- The BI tool can then be used for further aggregation or filtering.
select
    custom_tag_key_value_pairs_string,
    usage_date as time_key_day,
    usage_week as time_key_week,
    usage_month as time_key_month,
    sum(usage_usd) as usage_usd_sum
from usage_with_tag_string
group by 1, 2, 3, 4
order by 1, 2, 3, 4 