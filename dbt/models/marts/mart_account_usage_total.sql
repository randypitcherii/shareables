-- mart_account_usage_total.sql
-- Calculates total usage in USD and formats it as a string.
-- Original query had parameters for date and workspace, which are handled by upstream models or removed.

with base_usage as (
    select 
        usage_usd
    from {{ ref('fct_usage_with_workspace_and_pricing') }}
    -- Potentially add workspace filtering here if needed, though original dashboard param :param_workspace was for usage_filtered CTE
    -- where workspace_display_name = var('specific_workspace', '<ALL WORKSPACES>') -- Example if parameterization is desired
),

usage_total_calc as (
    select
        sum(usage_usd) as total_usage_usd_num
    from base_usage
)

select
    concat(
        'Total usage (USD): $ ',
        case
            when total_usage_usd_num >= 1e9 then concat(format_number(total_usage_usd_num / 1e9, 2), 'B')
            when total_usage_usd_num >= 1e6 then concat(format_number(total_usage_usd_num / 1e6, 2), 'M')
            when total_usage_usd_num >= 1e3 then concat(format_number(total_usage_usd_num / 1e3, 2), 'K')
            else format_number(total_usage_usd_num, 2)
        end
    ) as total_usage_usd_display
from usage_total_calc 