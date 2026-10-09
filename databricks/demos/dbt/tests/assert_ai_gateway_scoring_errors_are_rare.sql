-- ai_decide returned an error for at most 5% of the last day's scored rows.
-- A spike means the function is unavailable, quota is exhausted, or a rubric
-- edit broke the questions JSON -- all of which leave the loop silently blind.
{{ config(tags = ['ai_gateway_improvement', 'hourly'], severity = 'error') }}

select
    count(*) as scored,
    sum(case when md_error is not null or decision_metadata is null then 1 else 0 end) as errored
from {{ ref('ai_gateway__session_scores') }}
where scored_at >= current_timestamp() - interval 1 day
having count(*) > 0
   and sum(case when md_error is not null or decision_metadata is null then 1 else 0 end) > 0.05 * count(*)
