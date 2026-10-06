{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'session_id',
    on_schema_change = 'append_new_columns'
  )
}}

{#-
  Layer 4a: one bounded transcript summary per finished session whose model
  service logs payloads.

  The largest main-agent request of a session already carries the whole
  conversation (coding agents resend history every turn), so one payload
  row per session is enough. The ai_gateway_distill_transcript UDF turns it
  into <= ai_gateway_max_state_chars of JSON, redacting emails and secrets.

  Incremental: each run distills only finished sessions not yet present
  that have a payload row, highest estimated cost first, capped at 2x the
  scoring cap.
-#}

{%- set cap = 25 if var('deployment_environment') == 'ci_testing' else var('ai_gateway_max_sessions_per_run') | int -%}

with candidates as (
    select session_id, representative_request_id, representative_invocation_id, est_cost_usd
    from {{ ref('int_ai_gateway__session_metrics') }}
    where is_closed
      and has_payload_logging
      and representative_request_id is not null
    {% if is_incremental() %}
      and session_id not in (select session_id from {{ this }})
    {% endif %}
),

-- join to the payloads BEFORE applying the cap: a service can advertise an
-- inference table yet hold no row for a given request (sampling, logging
-- limits, a table this principal cannot read), and those sessions must not
-- use up the cap.
matched as (
    select
        c.session_id,
        c.est_cost_usd,
        p.inference_table,
        p.api_type,
        p.request,
        p.response,
        p.logging_error_codes
    from candidates c
    join {{ ref('int_ai_gateway__payloads') }} p
      on p.request_id = c.representative_request_id
    qualify row_number() over (
        partition by c.session_id
        order by case when p.invocation_id = c.representative_invocation_id then 0 else 1 end,
                 length(p.request) desc
    ) = 1
),

capped as (
    select * from matched
    order by est_cost_usd desc nulls last
    limit {{ cap * 2 }}
)

select
    session_id,
    inference_table,
    api_type,
    length(request) as request_chars,
    logging_error_codes,
    {{ function('ai_gateway_distill_transcript') }}(request, response, api_type, {{ var('ai_gateway_max_state_chars') }}) as transcript,
    current_timestamp() as distilled_at
from capped
