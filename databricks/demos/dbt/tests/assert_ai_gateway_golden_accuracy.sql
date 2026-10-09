-- The content rubric must stay accurate on the golden set. Floors are
-- deliberately below perfect: ai_decide is probabilistic and the eval is a
-- tripwire for a broken or drifted rubric, not a leaderboard.
{{ config(tags = ['ai_gateway_eval'], enabled = var('deployment_environment', 'development') != 'production') }}

with eval as (
    select * from {{ ref('ai_gateway__golden_eval') }}
),

accuracy as (
    select 'outcome' as question, avg(case when outcome_ok then 1.0 else 0 end) as accuracy, 0.75 as floor from eval
    union all select 'friction_presence', avg(case when friction_presence_ok then 1.0 else 0 end), 0.75 from eval
    union all select 'friction_exact', avg(case when friction_exact then 1.0 else 0 end), 0.6 from eval
    union all select 'goal_category', avg(case when goal_ok then 1.0 else 0 end), 0.6 from eval
    union all select 'frustration', avg(case when frustration_ok then 1.0 else 0 end), 0.6 from eval
    union all select 'repeated_instruction', avg(case when repeated_ok then 1.0 else 0 end), 0.75 from eval
)

select * from accuracy where accuracy < floor
