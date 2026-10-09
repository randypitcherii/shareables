{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'append_new_columns'
  )
}}

{#-
  Layer 4b: ai_decide scores, written ONCE per session per rubric version.

  WHY an append-only incremental table and not a materialized view:
  ai_decide is non-deterministic and costs money per row. A materialized view
  rejects non-deterministic functions for incremental refresh, so it would
  recompute -- and re-pay for -- every score on every refresh.

  Each run picks up to `ai_gateway_max_sessions_per_run` finished sessions:
    * unscored sessions with a transcript, then the costliest (1 - random share)
    * a uniform random sample of the remaining unscored   (random share)
    * a re-score of already-scored sessions, flagged
      is_stability_probe, to measure answer stability     (probe share)

  Two rubrics (macros/ai_gateway_improvement/rubrics.sql):
    * metadata -- every session, state = session metrics JSON
    * content  -- sessions with a distilled transcript, state = transcript
                  + headline metrics
  A session is re-picked when a transcript arrives after its metadata score,
  or when a rubric version changes. Downstream reads the latest non-probe row.
-#}

{%- set cap = 25 if var('deployment_environment') == 'ci_testing' else var('ai_gateway_max_sessions_per_run') | int -%}
{%- set n_random = (cap * var('ai_gateway_random_sample_share')) | int -%}
{%- set n_top = cap - n_random -%}
{%- set n_probe = [(cap * var('ai_gateway_stability_probe_share')) | int, 1] | max -%}
{%- set metadata_version = ai_gateway_rubric_version('metadata') -%}
{%- set content_version = ai_gateway_rubric_version('content') -%}

with sessions as (
    select m.*, t.transcript
    from {{ ref('int_ai_gateway__session_metrics') }} m
    left join {{ ref('int_ai_gateway__session_transcripts') }} t using (session_id)
    where m.is_closed
),

{% if is_incremental() %}
scored as (
    select
        session_id,
        max(case when metadata_rubric_version = '{{ metadata_version }}' then 1 else 0 end) = 1 as has_metadata,
        max(case when content_rubric_version = '{{ content_version }}' then 1 else 0 end) = 1 as has_content
    from {{ this }}
    where not is_stability_probe
    group by session_id
),
{% endif %}

unscored as (
    select sessions.*
    from sessions
    {% if is_incremental() %}
    left join scored using (session_id)
    where scored.session_id is null
       or not scored.has_metadata
       or (sessions.transcript is not null and not scored.has_content)
    {% endif %}
),

-- sessions with a transcript go first: content scores are the scarce,
-- high-value signal (only payload-logged services have them)
top_by_cost as (
    select *, 'top_cost' as selection_reason
    from unscored
    order by (transcript is not null) desc, est_cost_usd desc nulls last
    limit {{ n_top }}
),

random_sample as (
    select *, 'random_sample' as selection_reason
    from unscored
    where session_id not in (select session_id from top_by_cost)
    order by rand()
    limit {{ n_random }}
),

{% if is_incremental() %}
probe as (
    select sessions.*, 'stability_probe' as selection_reason
    from sessions
    join scored using (session_id)
    where scored.has_metadata
    order by rand()
    limit {{ n_probe }}
),
{% endif %}

picked as (
    select * from top_by_cost
    union all
    select * from random_sample
    {% if is_incremental() %}
    union all
    select * from probe
    {% endif %}
),

states as (
    select
        session_id,
        selection_reason,
        metadata_state,
        case when transcript is not null then
            concat(
                '{"transcript": ', transcript,
                ', "session": ', to_json(named_struct(
                    'client', client_family,
                    'model', primary_model,
                    'n_requests', n_requests,
                    'duration_minutes', round(duration_seconds / 60, 1),
                    'errors', n_errors
                )),
                '}'
            )
        end as content_state
    from picked
),

first_try as (
    select
        *,
        ai_decide(metadata_state, {{ ai_gateway_rubric_literal('metadata') }}) as d1_metadata,
        case when content_state is not null then
            ai_decide(content_state, {{ ai_gateway_rubric_literal('content') }})
        end as d1_content
    from states
),

-- ai_decide is in Beta and occasionally rejects its own answer (for example
-- an extra probability on a choice question), failing the whole call. One
-- retry on error recovers almost all of them; the answer is probabilistic,
-- so the retry is a fresh draw, not a repeat of the failure.
decided as (
    select
        session_id,
        selection_reason,
        content_state is not null as has_transcript,
        try_variant_get(d1_metadata, '$.error_message', 'string') is not null
            or try_variant_get(d1_content, '$.error_message', 'string') is not null as was_retried,
        case when try_variant_get(d1_metadata, '$.error_message', 'string') is not null
            then ai_decide(metadata_state, {{ ai_gateway_rubric_literal('metadata') }})
            else d1_metadata
        end as decision_metadata,
        case when try_variant_get(d1_content, '$.error_message', 'string') is not null
            then ai_decide(content_state, {{ ai_gateway_rubric_literal('content') }})
            else d1_content
        end as decision_content
    from first_try
)

select
    session_id,
    current_timestamp() as scored_at,
    selection_reason,
    selection_reason = 'stability_probe' as is_stability_probe,
    was_retried,
    '{{ metadata_version }}' as metadata_rubric_version,
    case when has_transcript then '{{ content_version }}' end as content_rubric_version,
    {{ ai_gateway_answer_columns('metadata', 'decision_metadata', 'md') }},
    {{ ai_gateway_answer_columns('content', 'decision_content', 'ct') }},
    decision_metadata,
    decision_content
from decided
