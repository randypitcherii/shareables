{#-
  Mart: one row per session -- metrics, the latest ai_decide answers, and
  transcript flags. The sessions explorer and every detector read this.

  Confidence gating: an answer below `ai_gateway_min_confidence` becomes
  `unclear` (choices) or NULL (scores) here, so no aggregate downstream ever
  counts a guess as a finding. The raw answers stay in session_scores.
-#}

with latest_scores as (
    select *
    from {{ ref('ai_gateway__session_scores') }}
    where not is_stability_probe
    qualify row_number() over (
        partition by session_id
        order by (content_rubric_version is not null) desc, scored_at desc
    ) = 1
)

select
    m.* except (metadata_state),
    t.session_id is not null as has_transcript,
    t.inference_table as transcript_inference_table,
    try_variant_get(parse_json(t.transcript), '$.first_user_request', 'string') as first_user_request,
    try_variant_get(parse_json(t.transcript), '$.n_user_turns', 'int') as n_user_turns,
    try_variant_get(parse_json(t.transcript), '$.n_tool_calls', 'int') as n_tool_calls,
    try_variant_get(parse_json(t.transcript), '$.n_tool_errors', 'int') as n_tool_errors,

    s.session_id is not null as is_scored,
    s.scored_at,
    s.selection_reason,
    s.metadata_rubric_version,
    s.content_rubric_version,

    -- metadata rubric (every scored session)
    {{ ai_gateway_gated_score('s.md_token_efficiency') }} as token_efficiency,
    {{ ai_gateway_gated_choice('s.md_model_fit') }} as model_fit,
    {{ ai_gateway_gated_score('s.md_context_pressure') }} as context_pressure,
    s.md_thrash as thrash_probability,

    -- content rubric (sessions with a transcript)
    {{ ai_gateway_gated_choice('s.ct_goal_category') }} as goal_category,
    {{ ai_gateway_gated_score('s.ct_complexity') }} as complexity,
    {{ ai_gateway_gated_choice('s.ct_outcome') }} as outcome,
    {{ ai_gateway_gated_score('s.ct_quality') }} as quality,
    {{ ai_gateway_gated_score('s.ct_user_frustration') }} as user_frustration,
    {{ ai_gateway_gated_choice('s.ct_primary_friction') }} as primary_friction,
    {{ ai_gateway_gated_score('s.ct_prompt_clarity') }} as prompt_clarity,
    s.ct_repeated_instruction as repeated_instruction_probability,
    s.ct_automation_candidate as automation_candidate_probability,
    s.ct_is_novel_friction as novel_friction_probability,
    s.ct_sensitive_data_present as sensitive_data_probability,

    coalesce(s.md_error, s.ct_error) as scoring_error
from {{ ref('int_ai_gateway__session_metrics') }} m
left join {{ ref('int_ai_gateway__session_transcripts') }} t using (session_id)
left join latest_scores s using (session_id)
