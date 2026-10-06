{{ config(materialized = 'streaming_table') }}

{#-
  Layer 1: an append-only mirror of system.ai_gateway.usage.

  WHY a streaming table, not a materialized view: system tables are served
  through Delta Sharing, and a materialized view over a shared table cannot
  refresh incrementally (EXPLAIN reports INPUT_NOT_IN_DELTA) -- it would
  recompute the whole window every hour. A streaming table reads each new
  usage row exactly once, and lands it in a real Delta table that every
  downstream materialized view CAN refresh incrementally.

  The first build loads `ai_gateway_history_days` of history (CI: 2 days).
  Later builds read only rows added since the last one.
-#}

{%- set history_days = 2 if var('deployment_environment') == 'ci_testing' else var('ai_gateway_history_days') -%}

select
    account_id,
    workspace_id,
    request_id,
    invocation_id,
    event_time,
    service_type,
    service_name,
    service_tags,
    endpoint_name,
    endpoint_tags,
    endpoint_metadata.inference_table as inference_table,
    destination_type,
    destination_name,
    destination_model,
    requester,
    requester_type,
    auth_mode,
    url,
    user_agent,
    api_type,
    request_tags,
    invocation_metadata,
    session_metadata,
    mcp_metadata,
    input_tokens,
    output_tokens,
    total_tokens,
    token_details,
    status_code,
    latency_ms,
    time_to_first_byte_ms,
    size(routing_information.attempts) as routing_attempts
from stream({{ source('system_ai_gateway', 'usage') }})
where event_time >= current_timestamp() - interval {{ history_days }} days
