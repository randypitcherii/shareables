{#-
  Mart: the ranked list of improvements to make. One row per detector x scope.

  Every detector is one CTE with the same output shape. Findings are never
  suppressed for low reach: a finding one person hits still shows. Ranking
  does the prioritizing:

    impact_score = log10(1 + est_monthly_savings_usd)
                 + log2(1 + affected_users)
                 + severity                                  (1 low .. 3 high)

  so a pain many people share outranks one person's expensive habit at the
  same dollar value. `people_impacted` is the badge the dashboard shows next
  to the dollar figure.

  Savings are list-price ESTIMATES (see the ai_gateway_model_prices seed) over
  the last `ai_gateway_recommendation_window_days`, scaled to 30 days.
  Thresholds are vars with defaults inline: override any in dbt_project.yml.
-#}

{%- set window_days = var('ai_gateway_recommendation_window_days', 30) -%}
{%- set monthly = '(30.0 / ' ~ window_days ~ ')' -%}

with sessions as (
    select * from {{ ref('ai_gateway__sessions') }}
    where started_at >= current_timestamp() - interval {{ window_days }} days
),

events as (
    select * from {{ ref('int_ai_gateway__usage_events') }}
    where event_time >= current_timestamp() - interval {{ window_days }} days
),

catalog as (
    select * from {{ ref('int_ai_gateway__model_catalog') }}
),

-- 0. Payload coverage gap: content scoring is blind to these services.
--    One account-level finding listing the biggest unlogged services, so it
--    does not crowd out everything else with one row per service.
unlogged_services as (
    select
        endpoint_name,
        sum(est_cost_usd) as est_cost_usd,
        count(*) as requests,
        count(distinct requester) as users
    from events
    where not has_payload_logging
      and service_type = 'MODEL_SERVICE'
      and invocation_source != 'GUARDRAIL'
    group by endpoint_name
    having count(*) >= {{ var('ai_gateway_min_requests_for_coverage', 20) }}
),

payload_coverage_gap as (
    select
        'payload_coverage_gap' as detector,
        'measurement' as category,
        'Enable inference tables on ' || cast(count(distinct u.endpoint_name) as string) || ' model services' as title,
        'account' as scope_type,
        'account' as scope_value,
        case when sum(u.est_cost_usd) / nullif((select sum(est_cost_usd) from events), 0) > 0.5 then 3 else 2 end as severity,
        cast(sum(u.requests) as bigint) as affected_sessions,
        (select count(distinct e.requester) from events e join unlogged_services x using (endpoint_name)
         where not e.has_payload_logging) as affected_users,
        cast(0 as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'unlogged_cost_share', round(sum(u.est_cost_usd) / nullif((select sum(est_cost_usd) from events), 0), 3),
            'top_services_by_cost', transform(
                slice(sort_array(collect_list(named_struct('c', u.est_cost_usd, 'name', u.endpoint_name)), false), 1, 10),
                x -> x.name)
        )) as evidence,
        cast(array() as array<string>) as example_session_ids,
        'Turn on inference tables for these model services, biggest spend first (AI Gateway > model service > '
        || 'Inference tables), so the loop can score quality, friction, and outcomes for their sessions.' as action
    from unlogged_services u
    having count(*) > 0
),

-- 1. Model overkill: premium/standard models on work a cheaper tier would do
model_overkill as (
    select
        'model_overkill' as detector,
        'cost' as category,
        'Route simpler ' || client_family || ' work off ' || primary_model as title,
        'client_model' as scope_type,
        client_family || ' / ' || primary_model as scope_value,
        2 as severity,
        count(*) as affected_sessions,
        count(distinct requester) as affected_users,
        cast(sum(est_cost_usd - coalesce(est_cost_tier_down_usd, est_cost_usd)) * {{ monthly }} as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'sessions_flagged', count(*),
            'by_judge', sum(case when model_fit = 'overpowered' then 1 else 0 end),
            'by_low_complexity', sum(case when complexity < 1.5 then 1 else 0 end),
            'est_cost_usd', round(sum(est_cost_usd), 2),
            'est_cost_one_tier_down_usd', round(sum(est_cost_tier_down_usd), 2)
        )) as evidence,
        transform(slice(sort_array(collect_list(named_struct('c', est_cost_usd, 'id', session_id)), false), 1, 5), x -> x.id) as example_session_ids,
        'These sessions look simpler than the model tier they ran on. Default this client to a cheaper '
        || 'model (or a smart-routing recipe) and keep the premium model for complex work.' as action
    from sessions
    where primary_model_tier in ('premium', 'standard')
      and est_cost_tier_down_usd is not null
      and (model_fit = 'overpowered' or complexity < 1.5)
    group by client_family, primary_model
),

-- 2. Missed prompt caching: long multi-turn sessions re-sending uncached input
missed_caching as (
    select
        'missed_prompt_caching' as detector,
        'cost' as category,
        'Prompt caching is barely used by ' || s.client_family || ' on ' || s.primary_model as title,
        'client_model' as scope_type,
        s.client_family || ' / ' || s.primary_model as scope_value,
        2 as severity,
        count(*) as affected_sessions,
        count(distinct s.requester) as affected_users,
        cast(sum(
            greatest(s.input_tokens * {{ var('ai_gateway_target_cache_hit_ratio', 0.7) }} - s.cache_read_tokens, 0)
            * (c.usd_per_mtok_input - c.usd_per_mtok_cache_read) / 1000000
        ) * {{ monthly }} as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'median_cache_hit_ratio', percentile_approx(s.cache_hit_ratio, 0.5),
            'uncached_input_tokens', sum(s.uncached_input_tokens),
            'target_cache_hit_ratio', {{ var('ai_gateway_target_cache_hit_ratio', 0.7) }}
        )) as evidence,
        transform(slice(sort_array(collect_list(named_struct('c', s.uncached_input_tokens, 'id', s.session_id)), false), 1, 5), x -> x.id) as example_session_ids,
        'Multi-turn sessions here resend large prompts without cache hits. Upgrade the client, enable '
        || 'prompt caching in its config, and keep system prompts and tool lists stable across turns.' as action
    from sessions s
    join catalog c on c.destination_model = s.primary_model
    where s.n_requests >= 3
      and s.input_tokens >= {{ var('ai_gateway_caching_min_input_tokens', 50000) }}
      and coalesce(s.cache_hit_ratio, 0) < {{ var('ai_gateway_low_cache_hit_ratio', 0.3) }}
    group by s.client_family, s.primary_model
),

-- 3. Context bloat: sessions running hot against the context window
context_bloat as (
    select
        'context_bloat' as detector,
        'efficiency' as category,
        'Sessions in ' || client_family || ' run near the context limit' as title,
        'client' as scope_type,
        client_family as scope_value,
        2 as severity,
        count(*) as affected_sessions,
        count(distinct requester) as affected_users,
        cast(sum(est_cost_usd) * {{ var('ai_gateway_context_bloat_savings_share', 0.2) }} * {{ monthly }} as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'max_input_tokens_p90', percentile_approx(max_input_tokens, 0.9),
            'median_growth_per_request', percentile_approx(input_growth_per_request, 0.5),
            'assumed_savings_share', {{ var('ai_gateway_context_bloat_savings_share', 0.2) }}
        )) as evidence,
        transform(slice(sort_array(collect_list(named_struct('c', max_input_tokens, 'id', session_id)), false), 1, 5), x -> x.id) as example_session_ids,
        'Context grows fast and stays large. Compact or reset long sessions, hand broad searches to '
        || 'subagents, and keep big files and logs out of the main conversation.' as action
    from sessions
    where context_pressure >= 2
       or (max_input_tokens >= {{ var('ai_gateway_context_bloat_tokens', 150000) }} and n_requests >= 5)
    group by client_family
),

-- 4. Retry storms: loops of errors, 429s, and fallbacks
retry_storms as (
    select
        'retry_storms' as detector,
        'reliability' as category,
        'Retry and rate-limit loops on ' || primary_model as title,
        'model' as scope_type,
        primary_model as scope_value,
        case when sum(n_rate_limited) > 100 then 3 else 2 end as severity,
        count(*) as affected_sessions,
        count(distinct requester) as affected_users,
        cast(sum(est_cost_usd * error_rate) * {{ monthly }} as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'rate_limited_requests', sum(n_rate_limited),
            'error_requests', sum(n_errors),
            'fallback_attempts', sum(n_fallback_attempts)
        )) as evidence,
        transform(slice(sort_array(collect_list(named_struct('c', n_errors, 'id', session_id)), false), 1, 5), x -> x.id) as example_session_ids,
        'Sessions here burn requests on errors and 429s. Raise the rate limit or add a fallback model on '
        || 'the model service, and check client retry and backoff settings.' as action
    from sessions
    where thrash_probability >= 0.7
       or n_rate_limited >= 3
       or (error_rate >= 0.2 and n_requests >= 5)
    group by primary_model
),

-- 5. Error hotspots: an endpoint x client version failing far above normal
error_hotspots as (
    select
        'error_hotspot' as detector,
        'reliability' as category,
        client_family || coalesce(' ' || client_version, '') || ' fails often on ' || endpoint_name as title,
        'endpoint_client_version' as scope_type,
        endpoint_name || ' / ' || client_family || ' ' || coalesce(client_version, '?') as scope_value,
        case when avg(case when is_error then 1.0 else 0 end) >= 0.25 then 3 else 2 end as severity,
        count(distinct request_id) as affected_sessions,
        count(distinct case when is_error then requester end) as affected_users,
        cast(0 as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'requests', count(*),
            'errors', sum(case when is_error then 1 else 0 end),
            'error_rate', round(avg(case when is_error then 1.0 else 0 end), 3),
            'top_status_codes', slice(array_distinct(collect_list(case when is_error then status_code end)), 1, 5)
        )) as evidence,
        cast(array() as array<string>) as example_session_ids,
        'Requests from this client version fail far more than normal. Pin or upgrade the client and check '
        || 'the model service config (payload limits, rate limits, model availability).' as action
    from events
    where service_type != 'MCP_SERVICE'
    group by endpoint_name, client_family, client_version
    having count(*) >= {{ var('ai_gateway_error_hotspot_min_requests', 50) }}
       and sum(case when is_error then 1 else 0 end) >= 20
       and avg(case when is_error then 1.0 else 0 end) >= {{ var('ai_gateway_error_hotspot_rate', 0.05) }}
),

-- 6. Stale coding-agent versions
versions as (
    select
        client_family,
        requester,
        max_by(client_version, started_at) as current_version,
        transform(split(regexp_extract(max_by(client_version, started_at), '^([0-9]+(\\.[0-9]+)*)', 1), '\\.'), v -> cast(v as int)) as current_semver
    from sessions
    where {{ ai_gateway_is_coding_agent('client_family') }}
      and client_version rlike '^[0-9]+\\.'
    group by client_family, requester
),

latest_versions as (
    select client_family, max(current_semver) as latest_semver
    from versions
    group by client_family
),

stale_versions as (
    select
        'stale_agent_version' as detector,
        'adoption' as category,
        'People run outdated ' || v.client_family || ' versions' as title,
        'client' as scope_type,
        v.client_family as scope_value,
        1 as severity,
        count(*) as affected_sessions,
        count(distinct v.requester) as affected_users,
        cast(0 as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'latest_version', array_join(transform(l.latest_semver, x -> cast(x as string)), '.'),
            'stale_versions', slice(array_distinct(collect_list(v.current_version)), 1, 8)
        )) as evidence,
        cast(array() as array<string>) as example_session_ids,
        'Upgrade these users to the newest release: newer coding-agent builds report session ids, use '
        || 'prompt caching better, and fix known bugs.' as action
    from versions v
    join latest_versions l using (client_family)
    where v.current_semver < l.latest_semver
      and size(l.latest_semver) > 0
      and l.latest_semver[0] = v.current_semver[0]  -- same major line only
    group by v.client_family, l.latest_semver
),

-- 7. PAT usage: long-lived tokens instead of OAuth
pat_usage as (
    select
        'pat_usage' as detector,
        'governance' as category,
        'Personal access tokens call the gateway' as title,
        'account' as scope_type,
        'account' as scope_value,
        3 as severity,
        count(distinct request_id) as affected_sessions,
        count(distinct requester) as affected_users,
        cast(0 as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'requests', count(*),
            'requesters', slice(array_distinct(collect_list(requester)), 1, 10),
            'clients', slice(array_distinct(collect_list(client_family)), 1, 10)
        )) as evidence,
        cast(array() as array<string>) as example_session_ids,
        'Move these callers to OAuth (U2M for people, M2M for service principals) and revoke the PATs.' as action
    from events
    where auth_mode = 'PAT'
    having count(*) > 0
),

-- 8. Unattributed spend: no team tag on the request or the service.
--    One account-level finding; the worst workspaces are in the evidence.
--    (Per-workspace rows swamp the list on accounts with hundreds of them.)
untagged_by_workspace as (
    select
        workspace_id,
        sum(case when team is null then est_cost_usd else 0 end) as untagged_cost,
        sum(est_cost_usd) as total_cost
    from events
    where service_type != 'MCP_SERVICE'
    group by workspace_id
    having sum(case when team is null then est_cost_usd else 0 end) > 0
),

untagged_spend as (
    select
        'untagged_spend' as detector,
        'governance' as category,
        'Spend with no team tag in ' || cast(count(*) as string) || ' workspaces' as title,
        'account' as scope_type,
        'account' as scope_value,
        case when sum(w.untagged_cost) / nullif(sum(w.total_cost), 0) > 0.5 then 2 else 1 end as severity,
        (select count(distinct request_id) from events where team is null and service_type != 'MCP_SERVICE') as affected_sessions,
        (select count(distinct requester) from events where team is null and service_type != 'MCP_SERVICE') as affected_users,
        cast(0 as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'untagged_cost_usd', round(sum(w.untagged_cost), 2),
            'untagged_share', round(sum(w.untagged_cost) / nullif(sum(w.total_cost), 0), 3),
            'top_workspaces_by_untagged_cost', transform(
                slice(sort_array(collect_list(named_struct('c', w.untagged_cost, 'ws', w.workspace_id)), false), 1, 10),
                x -> x.ws),
            'team_tag_keys', array({% for k in var('ai_gateway_team_tag_keys') %}'{{ k }}'{% if not loop.last %}, {% endif %}{% endfor %})
        )) as evidence,
        cast(array() as array<string>) as example_session_ids,
        'Tag model services with a team or cost-center tag, or send the Databricks-Ai-Gateway-Request-Tags '
        || 'header from clients, so spend can be attributed and charged back. Start with the workspaces in the evidence.' as action
    from untagged_by_workspace w
    having count(*) > 0
),

-- 9. Reasoning effort higher than the task needs
reasoning_mismatch as (
    select
        'reasoning_effort_mismatch' as detector,
        'cost' as category,
        'High reasoning effort on simple ' || client_family || ' work' as title,
        'client' as scope_type,
        client_family as scope_value,
        1 as severity,
        count(*) as affected_sessions,
        count(distinct s.requester) as affected_users,
        cast(sum(s.reasoning_tokens * c.usd_per_mtok_output / 1000000) * 0.5 * {{ monthly }} as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'reasoning_tokens', sum(s.reasoning_tokens),
            'efforts', slice(array_distinct(collect_list(s.reasoning_effort)), 1, 5),
            'assumed_reduction', 0.5
        )) as evidence,
        transform(slice(sort_array(collect_list(named_struct('c', s.reasoning_tokens, 'id', s.session_id)), false), 1, 5), x -> x.id) as example_session_ids,
        'Lower the default reasoning effort for this client and raise it per task when the work is hard.' as action
    from sessions s
    join catalog c on c.destination_model = s.primary_model
    where s.reasoning_effort in ('high', 'xhigh', 'max')
      and (s.complexity < 1.5 or s.model_fit = 'overpowered')
    group by client_family
),

-- 10. Guardrail overhead: workspaces where guardrail judge calls are a
--     large share of cost, rolled into one account-level finding.
guardrail_by_workspace as (
    select
        workspace_id,
        sum(case when invocation_source = 'GUARDRAIL' then est_cost_usd else 0 end) as guardrail_cost,
        sum(est_cost_usd) as total_cost
    from events
    group by workspace_id
    having sum(case when invocation_source = 'GUARDRAIL' then est_cost_usd else 0 end)
         / nullif(sum(est_cost_usd), 0) >= {{ var('ai_gateway_guardrail_share', 0.1) }}
),

guardrail_overhead as (
    select
        'guardrail_overhead' as detector,
        'cost' as category,
        'Guardrail checks are a large share of cost in ' || cast(count(*) as string) || ' workspaces' as title,
        'account' as scope_type,
        'account' as scope_value,
        1 as severity,
        (select count(distinct e.request_id) from events e join guardrail_by_workspace g using (workspace_id)
          where e.invocation_source = 'GUARDRAIL') as affected_sessions,
        (select count(distinct e.requester) from events e join guardrail_by_workspace g using (workspace_id)) as affected_users,
        cast(sum(w.guardrail_cost) * 0.5 * {{ monthly }} as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'guardrail_cost_usd', round(sum(w.guardrail_cost), 2),
            'guardrail_share_in_flagged_workspaces', round(sum(w.guardrail_cost) / nullif(sum(w.total_cost), 0), 3),
            'top_workspaces_by_guardrail_cost', transform(
                slice(sort_array(collect_list(named_struct('c', w.guardrail_cost, 'ws', w.workspace_id)), false), 1, 10),
                x -> x.ws),
            'assumed_reduction', 0.5
        )) as evidence,
        cast(array() as array<string>) as example_session_ids,
        'Guardrails add a judge call per request. Scope them to the services that need them, or point '
        || 'them at a smaller evaluator model.' as action
    from guardrail_by_workspace w
    having count(*) > 0
),

-- 11. Friction hotspots (content rubric)
friction_hotspots as (
    select
        'friction_hotspot' as detector,
        'quality' as category,
        replace(primary_friction, '_', ' ') || ' keeps happening in ' || client_family as title,
        'client_friction' as scope_type,
        client_family || ' / ' || primary_friction as scope_value,
        case when avg(user_frustration) >= 1.5 then 3 else 2 end as severity,
        count(*) as affected_sessions,
        count(distinct requester) as affected_users,
        cast(sum(est_cost_usd) * {{ var('ai_gateway_friction_rework_share', 0.25) }} * {{ monthly }} as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'sessions', count(*),
            'avg_frustration', round(avg(user_frustration), 2),
            'not_achieved', sum(case when outcome in ('not_achieved', 'partially_achieved') then 1 else 0 end),
            'assumed_rework_share', {{ var('ai_gateway_friction_rework_share', 0.25) }}
        )) as evidence,
        transform(slice(sort_array(collect_list(named_struct('c', coalesce(user_frustration, 0), 'id', session_id)), false), 1, 5), x -> x.id) as example_session_ids,
        case primary_friction
            when 'misunderstood_request' then 'Ask for a short plan before edits, and add project context to AGENTS.md / CLAUDE.md.'
            when 'wrong_approach' then 'Capture preferred approaches and patterns in AGENTS.md / CLAUDE.md or a skill.'
            when 'buggy_code' then 'Require tests to run before the agent declares success; add the test command to AGENTS.md.'
            when 'excessive_changes' then 'Add a rule to keep diffs minimal and scoped to the request.'
            when 'tool_failed' then 'Fix the failing tool or command, or document its correct usage for the agent.'
            when 'wrong_file_or_location' then 'Document the repo layout and where things live in AGENTS.md.'
            when 'slow_or_verbose' then 'Ask for concise output, or use a faster model for this kind of task.'
            when 'user_unclear' then 'Share a prompt template: goal, constraints, files, and done criteria.'
            when 'assistant_got_blocked' then 'Grant the missing permissions or tools, or document the workaround.'
            else 'Review the example sessions and turn the fix into a rule, a skill, or training.'
        end as action
    from sessions
    where primary_friction is not null
      and primary_friction not in ('no_friction', 'other', 'unclear')
    group by client_family, primary_friction
),

-- 12. Instructions people keep repeating -> codify them
repeated_instructions as (
    select
        'codify_repeated_instructions' as detector,
        'quality' as category,
        'People restate standing rules to ' || client_family as title,
        'client' as scope_type,
        client_family as scope_value,
        2 as severity,
        count(*) as affected_sessions,
        count(distinct requester) as affected_users,
        cast(0 as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'sessions', count(*),
            'sample_requests', slice(collect_list(substr(first_user_request, 1, 160)), 1, 3)
        )) as evidence,
        transform(slice(sort_array(collect_list(named_struct('c', repeated_instruction_probability, 'id', session_id)), false), 1, 5), x -> x.id) as example_session_ids,
        'Users repeat rules the agent should already know. Move them into AGENTS.md / CLAUDE.md or a '
        || 'shared skill so every session starts with them.' as action
    from sessions
    where repeated_instruction_probability >= {{ var('ai_gateway_noul_threshold', 0.7) }}
    group by client_family
),

-- 13. Task areas with poor outcomes
low_outcome_areas as (
    select
        'low_outcome_task_area' as detector,
        'quality' as category,
        replace(goal_category, '_', ' ') || ' sessions often fail' as title,
        'goal_category' as scope_type,
        goal_category as scope_value,
        case when avg(case when outcome in ('not_achieved', 'partially_achieved') then 1.0 else 0 end) >= 0.5 then 3 else 2 end as severity,
        count(*) as affected_sessions,
        count(distinct requester) as affected_users,
        cast(sum(case when outcome in ('not_achieved', 'partially_achieved') then est_cost_usd else 0 end) * {{ monthly }} as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'sessions', count(*),
            'failure_share', round(avg(case when outcome in ('not_achieved', 'partially_achieved') then 1.0 else 0 end), 3),
            'avg_quality', round(avg(quality), 2),
            'avg_complexity', round(avg(complexity), 2)
        )) as evidence,
        transform(slice(sort_array(collect_list(named_struct('c', case when outcome in ('not_achieved', 'partially_achieved') then 1 else 0 end, 'id', session_id)), false), 1, 5), x -> x.id) as example_session_ids,
        'This kind of task fails often. Give the agent better context or tools for it, try a stronger '
        || 'model here, or run an enablement session on how to frame these requests.' as action
    from sessions
    where goal_category is not null and goal_category not in ('unclear', 'other', 'warmup_minimal')
      and outcome is not null and outcome != 'unclear'
    group by goal_category
    having count(*) >= 3
       and avg(case when outcome in ('not_achieved', 'partially_achieved') then 1.0 else 0 end) >= {{ var('ai_gateway_low_outcome_share', 0.3) }}
),

-- 14. Automation candidates
automation_candidates as (
    select
        'automation_candidate' as detector,
        'adoption' as category,
        'Repeated ' || replace(goal_category, '_', ' ') || ' work could become a skill' as title,
        'goal_category' as scope_type,
        goal_category as scope_value,
        1 as severity,
        count(*) as affected_sessions,
        count(distinct requester) as affected_users,
        cast(0 as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct(
            'sessions', count(*),
            'sample_requests', slice(collect_list(substr(first_user_request, 1, 160)), 1, 3)
        )) as evidence,
        transform(slice(sort_array(collect_list(named_struct('c', automation_candidate_probability, 'id', session_id)), false), 1, 5), x -> x.id) as example_session_ids,
        'These sessions repeat a well-defined workflow. Capture it as a skill, script, or template.' as action
    from sessions
    where automation_candidate_probability >= {{ var('ai_gateway_noul_threshold', 0.7) }}
      and goal_category is not null
    group by goal_category
),

-- 15. Taxonomy drift: friction the rubric has no label for
novel_friction as (
    select
        'novel_friction' as detector,
        'measurement' as category,
        'Friction the rubric cannot label' as title,
        'account' as scope_type,
        'account' as scope_value,
        1 as severity,
        count(*) as affected_sessions,
        count(distinct requester) as affected_users,
        cast(0 as decimal(38, 4)) as est_monthly_savings_usd,
        to_json(named_struct('sessions', count(*))) as evidence,
        transform(slice(sort_array(collect_list(named_struct('c', novel_friction_probability, 'id', session_id)), false), 1, 5), x -> x.id) as example_session_ids,
        'Read the example sessions. If a new friction pattern shows up, add it to the content rubric '
        || '(macros/ai_gateway_improvement/rubrics.sql).' as action
    from sessions
    where novel_friction_probability >= {{ var('ai_gateway_noul_threshold', 0.7) }}
    having count(*) > 0
),

findings as (
    select * from payload_coverage_gap
    union all select * from model_overkill
    union all select * from missed_caching
    union all select * from context_bloat
    union all select * from retry_storms
    union all select * from error_hotspots
    union all select * from stale_versions
    union all select * from pat_usage
    union all select * from untagged_spend
    union all select * from reasoning_mismatch
    union all select * from guardrail_overhead
    union all select * from friction_hotspots
    union all select * from repeated_instructions
    union all select * from low_outcome_areas
    union all select * from automation_candidates
    union all select * from novel_friction
)

select
    md5(detector || '|' || scope_type || '|' || scope_value) as recommendation_id,
    detector,
    category,
    title,
    scope_type,
    scope_value,
    severity,
    affected_sessions,
    affected_users as people_impacted,
    case
        when affected_users >= 10 then '🏢 10+'
        when affected_users >= 2 then '👥 2-9'
        else '👤 1'
    end as people_impacted_band,
    greatest(coalesce(est_monthly_savings_usd, 0), 0) as est_monthly_savings_usd,
    cast(
        log10(1 + greatest(coalesce(est_monthly_savings_usd, 0), 0))
        + log2(1 + affected_users)
        + severity
    as decimal(10, 3)) as impact_score,
    evidence,
    example_session_ids,
    action,
    current_timestamp() as generated_at
from findings
where affected_sessions > 0
