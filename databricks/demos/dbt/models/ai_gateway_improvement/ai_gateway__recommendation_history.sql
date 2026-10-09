{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'append_new_columns'
  )
}}

{#-
  One snapshot of the recommendations per run, so the dashboard can show
  whether a finding is growing or shrinking ("vs last week") and when it
  first appeared. Append-only; ~a few hundred rows per hourly run.
-#}

select
    recommendation_id,
    detector,
    category,
    scope_type,
    scope_value,
    severity,
    affected_sessions,
    people_impacted,
    est_monthly_savings_usd,
    impact_score,
    generated_at as snapshot_at
from {{ ref('ai_gateway__recommendations') }}
