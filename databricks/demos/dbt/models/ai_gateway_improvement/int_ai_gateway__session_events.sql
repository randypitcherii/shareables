{{ config(materialized = 'materialized_view') }}

{#-
  Layer 3a: assign every model invocation to a session.

  Fewer than 1% of gateway rows carry a client session id: coding agents
  that report one (recent Claude Code, Codex) do, everything else does not.
  So the session id is:

    * native    -- `n:` + session_metadata.client_session_id when present.
                   Subagent calls share the parent's id and roll up into it.
    * synthetic -- for everything else: one requester + workspace + client
                   family + invocation source, cut into a new session after
                   `ai_gateway_session_gap_minutes` of silence.

  Guardrail calls (invocation_source GUARDRAIL) and MCP tool traffic are not
  sessions of their own and are excluded here; recommendations read them
  from usage_events directly.

  Window functions over an append-only source: EXPLAIN confirms this view
  refreshes incrementally.
-#}

with events as (
    select *
    from {{ ref('int_ai_gateway__usage_events') }}
    where service_type != 'MCP_SERVICE'
      and invocation_source != 'GUARDRAIL'
),

keyed as (
    select
        *,
        concat_ws('|', requester, workspace_id, client_family, invocation_source) as synthetic_base
    from events
),

gaps as (
    select
        *,
        case
            when native_session_id is not null then 0
            when lag(event_time) over (partition by synthetic_base order by event_time, request_id) is null then 1
            when event_time > lag(event_time) over (partition by synthetic_base order by event_time, request_id)
                + interval {{ var('ai_gateway_session_gap_minutes') }} minutes then 1
            else 0
        end as starts_session
    from keyed
),

numbered as (
    select
        *,
        sum(starts_session) over (
            partition by synthetic_base order by event_time, request_id
            rows between unbounded preceding and current row
        ) as synthetic_seq
    from gaps
)

select
    case
        when native_session_id is not null then concat('n:', native_session_id)
        else concat('s:', sha2(concat(synthetic_base, '#', cast(synthetic_seq as string)), 256))
    end as session_id,
    case when native_session_id is not null then 'native' else 'synthetic' end as session_id_source,
    * except (synthetic_base, starts_session, synthetic_seq)
from numbered
