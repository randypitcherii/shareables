{{ config(materialized = 'materialized_view') }}

{#-
  Layer 3b: one row per session with the metrics every rubric and detector
  reads. Deterministic aggregates only, money in DECIMAL, so the view
  refreshes incrementally. "Is this session finished?" depends on the clock,
  so it is decided downstream, never here.
-#}

with events as (
    select
        *,
        case model_tier when 'premium' then 3 when 'standard' then 2 when 'economy' then 1 else 0 end as tier_rank
    from {{ ref('int_ai_gateway__session_events') }}
)

select
    session_id,
    max(session_id_source) as session_id_source,
    max(account_id) as account_id,
    max_by(workspace_id, event_time) as workspace_id,
    max_by(requester, event_time) as requester,
    max_by(requester_type, event_time) as requester_type,
    max(team) as team,
    max_by(client_family, event_time) as client_family,
    max_by(client_version, event_time) as client_version,
    max_by(client_surface, event_time) as client_surface,
    max_by(invocation_source, event_time) as invocation_source,
    max_by(auth_mode, event_time) as auth_mode,
    max_by(reasoning_effort, event_time) as reasoning_effort,
    max(smart_router_name) as smart_router_name,

    -- time
    min(event_time) as started_at,
    max(event_time) as ended_at,
    cast(min(event_time) as date) as session_date,
    timestampdiff(SECOND, min(event_time), max(event_time)) as duration_seconds,

    -- volume
    count(*) as n_requests,
    count(distinct subagent_id) as n_subagents,
    count(distinct destination_model) as n_models,
    max_by(destination_model, est_cost_usd) as primary_model,
    max_by(model_family, est_cost_usd) as primary_model_family,
    max_by(model_tier, est_cost_usd) as primary_model_tier,
    max(tier_rank) as max_tier_rank,

    -- tokens
    sum(input_tokens) as input_tokens,
    sum(output_tokens) as output_tokens,
    sum(cache_read_tokens) as cache_read_tokens,
    sum(cache_write_tokens) as cache_write_tokens,
    sum(uncached_input_tokens) as uncached_input_tokens,
    sum(reasoning_tokens) as reasoning_tokens,
    max(input_tokens) as max_input_tokens,
    min_by(input_tokens, event_time) as first_input_tokens,

    -- reliability
    sum(case when is_error then 1 else 0 end) as n_errors,
    sum(case when is_rate_limited then 1 else 0 end) as n_rate_limited,
    sum(fallback_attempts) as n_fallback_attempts,
    sum(latency_ms) as total_latency_ms,

    -- cost (list-price estimates)
    sum(est_cost_usd) as est_cost_usd,
    sum(est_cost_tier_down_usd) as est_cost_tier_down_usd,
    sum(est_cost_no_cache_usd) as est_cost_no_cache_usd,
    sum(case when is_unpriced then 1 else 0 end) as n_unpriced_requests,

    -- payloads: the largest main-agent request holds the full conversation
    max(case when has_payload_logging then 1 else 0 end) = 1 as has_payload_logging,
    max(inference_table) as inference_table,
    max_by(request_id, case when subagent_id is null then input_tokens else -1 end) as representative_request_id,
    max_by(invocation_id, case when subagent_id is null then input_tokens else -1 end) as representative_invocation_id
from events
group by session_id
