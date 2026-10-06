{#-
  Mart: is the measurement itself trustworthy? Long format, one row per
  metric, so the dashboard's health page is a filterable table.

  Groups:
    freshness  -- how far behind the system table the mirror is
    coverage   -- share of sessions (and spend) that are scored / have transcripts
    confidence -- share of answers below the gating threshold, per question
    stability  -- agreement between a stability-probe re-score and the original
    anchors    -- does each judged score track its deterministic twin?
    cost       -- unpriced traffic, and the estimate vs billed list cost
-#}

{%- set window_days = var('ai_gateway_recommendation_window_days', 30) -%}
{%- set min_conf = var('ai_gateway_min_confidence') -%}

with sessions as (
    select * from {{ ref('ai_gateway__sessions') }}
    where started_at >= current_timestamp() - interval {{ window_days }} days
),

scores as (
    select * from {{ ref('ai_gateway__session_scores') }}
    where scored_at >= current_timestamp() - interval {{ window_days }} days
),

events as (
    select * from {{ ref('int_ai_gateway__usage_events') }}
    where event_time >= current_timestamp() - interval {{ window_days }} days
),

probe_pairs as (
    select p.*, o.md_model_fit as o_model_fit, o.md_token_efficiency as o_token_efficiency,
           o.ct_outcome as o_outcome,
           o.ct_primary_friction as o_primary_friction, o.ct_complexity as o_complexity
    from scores p
    join (
        select * from scores where not is_stability_probe
        qualify row_number() over (partition by session_id order by scored_at) = 1
    ) o using (session_id)
    where p.is_stability_probe
),

billing as (
    select sum(list_cost) as billed_list_cost
    from {{ ref('int_usage_priced') }}
    where usage_type = 'TOKEN'
      and usage_date >= current_date() - {{ window_days }}
),

metrics as (
    select 'freshness' as metric_group, 'minutes_since_latest_event' as metric,
           cast(timestampdiff(MINUTE, max(event_time), current_timestamp()) as double) as value,
           'Expect < 90: system.ai_gateway.usage itself lags up to an hour.' as detail
    from {{ ref('stg_ai_gateway__usage') }}

    union all
    select 'coverage', 'sessions_in_window', cast(count(*) as double), 'Sessions started in the window.' from sessions
    union all
    select 'coverage', 'scored_session_share', avg(case when is_scored then 1.0 else 0 end), 'Finished sessions with an ai_decide score.' from sessions where is_closed
    union all
    select 'coverage', 'scored_cost_share', sum(case when is_scored then est_cost_usd else 0 end) / nullif(sum(est_cost_usd), 0), 'Share of estimated spend covered by scored sessions (top-cost-first selection makes this higher than the session share).' from sessions where is_closed
    union all
    select 'coverage', 'payload_logged_session_share', avg(case when has_payload_logging then 1.0 else 0 end), 'Sessions whose model service logs payloads. Raise it by enabling inference tables (see payload_coverage_gap).' from sessions
    union all
    select 'coverage', 'payload_logged_cost_share', sum(case when has_payload_logging then est_cost_usd else 0 end) / nullif(sum(est_cost_usd), 0), 'Share of estimated spend on services that log payloads.' from sessions
    union all
    select 'coverage', 'transcript_session_share', avg(case when has_transcript then 1.0 else 0 end), 'Sessions with a readable, distilled transcript (content-scored).' from sessions where is_closed
    union all
    select 'coverage', 'native_session_id_share', avg(case when session_id_source = 'native' then 1.0 else 0 end), 'Sessions keyed by a client session id rather than the idle-gap rule.' from sessions
    union all
    select 'coverage', 'scoring_error_count', cast(sum(case when scoring_error is not null then 1 else 0 end) as double), 'ai_decide calls that returned error_message.' from sessions

    {%- for col in ['md_token_efficiency', 'md_model_fit', 'md_context_pressure', 'ct_goal_category', 'ct_complexity', 'ct_outcome', 'ct_quality', 'ct_user_frustration', 'ct_primary_friction', 'ct_prompt_clarity'] %}
    union all
    select 'confidence', '{{ col }}_low_confidence_share',
           avg(case when {{ col }}_confidence < {{ min_conf }} then 1.0 else 0 end),
           'Answers below confidence {{ min_conf }}; aggregated as unclear.'
    from scores where {{ col }} is not null
    {%- endfor %}

    union all
    select 'stability', 'probe_pairs', cast(count(*) as double), 'Re-scored sessions in the window.' from probe_pairs
    union all
    select 'stability', 'model_fit_agreement', avg(case when md_model_fit = o_model_fit then 1.0 else 0 end), 'Same choice on re-score.' from probe_pairs
    union all
    select 'stability', 'token_efficiency_within_half_point', avg(case when abs(md_token_efficiency - o_token_efficiency) <= 0.5 then 1.0 else 0 end), 'Score within 0.5 on re-score.' from probe_pairs
    union all
    select 'stability', 'outcome_agreement', avg(case when ct_outcome = o_outcome then 1.0 else 0 end), 'Same choice on re-score (content rubric).' from probe_pairs where ct_outcome is not null
    union all
    select 'stability', 'primary_friction_agreement', avg(case when ct_primary_friction = o_primary_friction then 1.0 else 0 end), 'Same choice on re-score (content rubric).' from probe_pairs where ct_primary_friction is not null

    union all
    select 'anchors', 'token_efficiency_vs_cache_hit_corr', corr(token_efficiency, cast(cache_hit_ratio as double)), 'Positive = the judge agrees with the cache hit ratio.' from sessions where token_efficiency is not null
    union all
    select 'anchors', 'context_pressure_vs_max_input_corr', corr(context_pressure, cast(max_input_tokens as double)), 'Positive = the judge agrees with max input tokens.' from sessions where context_pressure is not null
    union all
    select 'anchors', 'thrash_vs_error_rate_corr', corr(thrash_probability, cast(error_rate as double)), 'Positive = the judge agrees with the observed error rate.' from sessions where thrash_probability is not null
    union all
    select 'anchors', 'tool_errors_vs_friction_tool_failed', avg(case when primary_friction = 'tool_failed' then 1.0 else 0 end), 'Share labelled tool_failed among sessions with 3+ tool errors.' from sessions where n_tool_errors >= 3

    union all
    select 'cost', 'unpriced_request_share', avg(case when is_unpriced then 1.0 else 0 end), 'Requests priced by the catch-all seed row; add their models to ai_gateway_model_prices.' from events where service_type != 'MCP_SERVICE'
    union all
    select 'cost', 'estimated_cost_usd', cast(sum(est_cost_usd) as double), 'List-price estimate from the seed, all gateway model traffic in the window.' from events
    union all
    select 'cost', 'billed_token_list_cost_usd', cast(billed_list_cost as double), 'system.billing list cost of token-billed serving in the window (includes non-gateway traffic).' from billing
)

select
    metric_group,
    metric,
    round(value, 4) as value,
    detail,
    current_timestamp() as computed_at
from metrics
