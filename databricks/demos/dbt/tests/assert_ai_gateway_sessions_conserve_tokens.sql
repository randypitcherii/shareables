-- Sessionization must not drop or duplicate tokens: the session totals equal
-- the event totals they were built from (model traffic, no guardrails/MCP).
{{ config(tags = ['ai_gateway_improvement', 'hourly']) }}

with events as (
    select coalesce(sum(input_tokens), 0) as input_tokens, coalesce(sum(output_tokens), 0) as output_tokens
    from {{ ref('int_ai_gateway__session_events') }}
),

sessions as (
    select coalesce(sum(input_tokens), 0) as input_tokens, coalesce(sum(output_tokens), 0) as output_tokens
    from {{ ref('int_ai_gateway__sessions') }}
)

select *
from events cross join sessions s
where events.input_tokens != s.input_tokens
   or events.output_tokens != s.output_tokens
