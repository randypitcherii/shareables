{{ config(materialized = 'view') }}

{#-
  Sessions plus the derived ratios, the clock-dependent `is_closed` flag,
  and the deterministic anchors each ai_decide score is checked against.
  A plain view: cheap, and free to use current_timestamp().
-#}

select
    s.*,
    s.ended_at < current_timestamp() - interval {{ var('ai_gateway_session_gap_minutes') }} minutes as is_closed,

    -- ratios (deterministic anchors for the metadata rubric)
    cast(s.cache_read_tokens / nullif(s.input_tokens, 0) as decimal(10, 4)) as cache_hit_ratio,
    cast(s.uncached_input_tokens / nullif(s.input_tokens, 0) as decimal(10, 4)) as uncached_input_share,
    cast(s.output_tokens / nullif(s.input_tokens, 0) as decimal(18, 6)) as output_input_ratio,
    cast((s.max_input_tokens - s.first_input_tokens) / greatest(s.n_requests - 1, 1) as decimal(18, 2)) as input_growth_per_request,
    cast(s.n_errors / s.n_requests as decimal(10, 4)) as error_rate,
    cast(s.est_cost_usd / nullif(s.n_requests, 0) as decimal(38, 10)) as est_cost_per_request_usd,

    -- rule-based workload type (the ai_decide answer is checked against it)
    case
        when s.invocation_source = 'AI_QUERY' then 'batch_inference'
        when s.invocation_source in ('AI_PLAYGROUND') then 'exploration'
        when s.invocation_source in ('MANAGED_AGENT', 'ROUTER') then 'agent_pipeline'
        when {{ ai_gateway_is_coding_agent('s.client_family') }} and s.requester_type = 'USER' then 'interactive_coding'
        when s.invocation_source = 'DATABRICKS_APPS' then 'app_backend'
        when s.requester_type != 'USER' then 'agent_pipeline'
        else 'other'
    end as workload_type_rule,

    -- the compact JSON state the metadata rubric judges
    to_json(named_struct(
        'client', s.client_family,
        'invocation_source', s.invocation_source,
        'requester_type', s.requester_type,
        'model', s.primary_model,
        'model_tier', s.primary_model_tier,
        'n_models', s.n_models,
        'reasoning_effort', s.reasoning_effort,
        'duration_minutes', round(s.duration_seconds / 60, 1),
        'n_requests', s.n_requests,
        'n_subagents', s.n_subagents,
        'input_tokens', s.input_tokens,
        'output_tokens', s.output_tokens,
        'uncached_input_tokens', s.uncached_input_tokens,
        'cache_read_tokens', s.cache_read_tokens,
        'cache_write_tokens', s.cache_write_tokens,
        'reasoning_tokens', s.reasoning_tokens,
        'cache_hit_ratio', round(s.cache_read_tokens / nullif(s.input_tokens, 0), 3),
        'max_input_tokens', s.max_input_tokens,
        'first_input_tokens', s.first_input_tokens,
        'input_growth_per_request', round((s.max_input_tokens - s.first_input_tokens) / greatest(s.n_requests - 1, 1)),
        'errors', s.n_errors,
        'rate_limited', s.n_rate_limited,
        'fallback_attempts', s.n_fallback_attempts,
        'est_cost_usd', round(s.est_cost_usd, 4)
    )) as metadata_state
from {{ ref('int_ai_gateway__sessions') }} s
