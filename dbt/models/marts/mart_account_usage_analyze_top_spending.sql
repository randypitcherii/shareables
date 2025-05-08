-- mart_account_usage_analyze_top_spending.sql
-- Analyzes top spending by a dynamically chosen rank key.
-- Original query used :param_rank_key (e.g., 'workspace', 'run_as', or usage_metadata fields) and :param_top_n.
-- This dbt model will provide usage grouped by several common rank keys. 
-- The BI tool can then filter/rank. For a fully dynamic rank_key, a more complex model or BI-layer logic is needed.

with base_usage as (
    select 
        usage_usd,
        usage_date,   -- for day
        usage_week,   -- for week
        usage_month,  -- for month
        workspace_display_name,
        -- Assuming identity_metadata.run_as is the relevant field for 'run_as' ranking.
        -- Adjust if the actual field is different.
        identity_metadata.run_as as run_as_user, 
        -- For usage_metadata fields, these would need to be explicitly extracted if they are structs.
        -- Example for job_id if it's in usage_metadata:
        usage_metadata.job_id as job_id,
        usage_metadata.notebook_id as notebook_id,
        sku_name,
        billing_origin_product
    from {{ ref('fct_usage_with_workspace_and_pricing') }}
    -- Original query parameters :param_show_null_rank_key is handled by not filtering out nulls here.
    -- :param_top_n would be applied in the BI tool or a more specific final mart.
)

-- This mart provides usage data with several potential keys for ranking.
-- The BI tool can then be used to select the rank_key, aggregate, and apply top N.
select
    -- Time keys
    usage_date as time_key_day,
    usage_week as time_key_week,
    usage_month as time_key_month,

    -- Potential Rank Keys
    workspace_display_name,
    run_as_user,
    job_id,
    notebook_id,
    sku_name,
    billing_origin_product,
    
    -- Metric
    usage_usd
from base_usage 