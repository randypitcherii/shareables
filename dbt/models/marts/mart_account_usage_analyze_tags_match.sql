-- mart_account_usage_analyze_tags_match.sql
-- Calculates the percentage of usage that matches specified tag criteria.
-- The original query had parameters for tag_entries and single_tag_key.
-- For the dbt model, we might make these dbt variables or focus on a common use case.
-- Here, we assume a simplified approach where we look for specific tags if defined, or all if not.

-- For simplicity, this dbt model will not replicate the complex tag parsing from parameters.
-- It will provide total usage and usage that has *any* custom tags, as a starting point.
-- More specific tag matching logic would require either dbt variables for tag keys/values 
-- or a more sophisticated parsing model if the dashboard's input format is to be exactly replicated.

with base_usage as (
    select 
        usage_usd,
        custom_tags -- This is a map type
    from {{ ref('fct_usage_with_workspace_and_pricing') }}
),

aggregated_usage as (
    select
        sum(usage_usd) as total_usage_usd,
        sum(case when size(custom_tags) > 0 then usage_usd else 0 end) as usage_with_any_tags_usd
    from base_usage
)

select
    total_usage_usd,
    usage_with_any_tags_usd,
    {{ dbt_utils.safe_divide('usage_with_any_tags_usd * 100', 'total_usage_usd') }} as percentage_usage_with_any_tags,
    'Note: This model calculates usage with ANY custom tags. Original dashboard had dynamic tag filtering.' as comment
from aggregated_usage 