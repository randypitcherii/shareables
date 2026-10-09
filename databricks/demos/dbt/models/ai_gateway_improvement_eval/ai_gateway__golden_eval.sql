{#-
  Score the golden set with the LIVE content rubric and compare to labels.
  On demand only: `make ai-gateway-eval` (tag ai_gateway_eval).
  tests/assert_ai_gateway_golden_accuracy.sql turns this into a pass/fail.
-#}

with golden as (
    select * from {{ ref('ai_gateway_golden_sessions') }}
),

states as (
    select
        golden.*,
            concat(
                '{"transcript": ', to_json(named_struct(
                    'n_user_turns', 1 + size(filter(split(coalesce(later_user_turns, ''), '[|]'), x -> x != '')),
                    'n_tool_errors', n_tool_errors,
                    'first_user_request', first_user_request,
                    'recent_user_turns', filter(split(coalesce(later_user_turns, ''), '[|]'), x -> x != ''),
                    'final_assistant_reply', final_assistant_reply
                )),
                ', "session": {"client": "claude-code"}}'
            ) as content_state
    from golden
),

first_try as (
    select *, ai_decide(content_state, {{ ai_gateway_rubric_literal('content') }}) as d1
    from states
),

-- same single retry on error as ai_gateway__session_scores
scored as (
    select
        *,
        case when try_variant_get(d1, '$.error_message', 'string') is not null
            then ai_decide(content_state, {{ ai_gateway_rubric_literal('content') }})
            else d1
        end as decision
    from first_try
),

answers as (
    select
        *,
        {{ ai_gateway_answer_columns('content', 'decision', 'ct') }}
    from scored
)

select
    golden_id,
    expected_outcome, ct_outcome,
    expected_primary_friction, ct_primary_friction,
    expected_goal_category, ct_goal_category,
    expected_frustration_min, expected_frustration_max, ct_user_frustration,
    expected_repeated_instruction, ct_repeated_instruction,
    -- outcome: exact, or adjacent on the achieved scale
    ct_outcome = expected_outcome
        or (expected_outcome in ('fully_achieved', 'mostly_achieved') and ct_outcome in ('fully_achieved', 'mostly_achieved'))
        or (expected_outcome in ('not_achieved', 'partially_achieved') and ct_outcome in ('not_achieved', 'partially_achieved'))
        as outcome_ok,
    coalesce(ct_primary_friction = expected_primary_friction, false) as friction_exact,
    (expected_primary_friction = 'no_friction') = (ct_primary_friction = 'no_friction') as friction_presence_ok,
    ct_goal_category = expected_goal_category as goal_ok,
    ct_user_frustration between expected_frustration_min and expected_frustration_max as frustration_ok,
    (ct_repeated_instruction >= 0.5) = (expected_repeated_instruction = 'yes') as repeated_ok,
    decision
from answers
