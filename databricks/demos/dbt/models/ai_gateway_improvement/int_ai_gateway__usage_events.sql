{{ config(materialized = 'materialized_view') }}

{#-
  Layer 2: one row per gateway invocation, normalized and priced.

  Materialized view over the streaming mirror: refreshes incrementally
  (append-only Delta source, equi-join to the model catalog, deterministic
  expressions only, DECIMAL not DOUBLE for money).

  Token accounting: the gateway's input_tokens INCLUDES cached tokens (cache
  reads never exceed it on any API shape), so uncached input =
  input - cache_read - cache_write, floored at zero.
-#}

with usage as (
    select * from {{ ref('stg_ai_gateway__usage') }}
),

catalog as (
    select * from {{ ref('int_ai_gateway__model_catalog') }}
),

normalized as (
    select
        usage.account_id,
        usage.workspace_id,
        usage.request_id,
        usage.invocation_id,
        usage.event_time,
        cast(usage.event_time as date) as event_date,

        -- who
        usage.requester,
        usage.requester_type,
        usage.auth_mode,
        {{ ai_gateway_team('usage.request_tags', 'usage.service_tags') }} as team,
        usage.request_tags,

        -- what was called
        coalesce(usage.service_type, 'MODEL_SERVICE') as service_type,
        coalesce(usage.invocation_metadata.source, 'UNKNOWN') as invocation_source,
        usage.invocation_metadata.service_tier as provider_service_tier,
        usage.endpoint_name,
        usage.service_name,
        usage.api_type,
        usage.destination_type,
        coalesce(usage.destination_model, usage.destination_name, usage.endpoint_name) as destination_model,
        usage.inference_table,
        usage.inference_table is not null and usage.inference_table != '' as has_payload_logging,
        usage.mcp_metadata.tool_name as mcp_tool_name,
        usage.mcp_metadata.json_rpc_method as mcp_method,

        -- client
        {{ ai_gateway_client_family('usage.session_metadata.coding_agent', 'usage.user_agent') }} as client_family,
        -- the user-agent token is a version only for named tools: `Mozilla/5.0`
        -- is the browser convention, not a release anyone can upgrade
        coalesce(
            usage.session_metadata.agent_version,
            case when usage.user_agent not rlike '(?i)^Mozilla/'
                 then nullif(regexp_extract(usage.user_agent, '^[^/]+/([^ ]+)', 1), '') end
        ) as client_version,
        usage.session_metadata.surface as client_surface,
        usage.user_agent,
        nullif(usage.session_metadata.client_session_id, '') as native_session_id,
        nullif(usage.session_metadata.client_subagent_id, '') as subagent_id,
        lower(usage.session_metadata.reasoning_effort) as reasoning_effort,
        usage.session_metadata.smart_router_name as smart_router_name,

        -- tokens
        coalesce(usage.input_tokens, 0) as input_tokens,
        coalesce(usage.output_tokens, 0) as output_tokens,
        coalesce(usage.token_details.cache_read_input_tokens, 0) as cache_read_tokens,
        coalesce(usage.token_details.cache_creation_input_tokens, 0) as cache_write_tokens,
        coalesce(usage.token_details.output_reasoning_tokens, 0) as reasoning_tokens,
        greatest(
            coalesce(usage.input_tokens, 0)
            - coalesce(usage.token_details.cache_read_input_tokens, 0)
            - coalesce(usage.token_details.cache_creation_input_tokens, 0),
            0
        ) as uncached_input_tokens,

        -- outcome
        usage.status_code,
        usage.status_code >= 400 as is_error,
        usage.status_code = 429 as is_rate_limited,
        usage.latency_ms,
        usage.time_to_first_byte_ms,
        greatest(coalesce(usage.routing_attempts, 1) - 1, 0) as fallback_attempts
    from usage
)

select
    normalized.*,
    catalog.model_family,
    coalesce(catalog.model_tier, 'standard') as model_tier,
    coalesce(catalog.is_unpriced, true) as is_unpriced,

    -- list-price estimate (see the ai_gateway_model_prices seed)
    cast((
        normalized.uncached_input_tokens * catalog.usd_per_mtok_input
        + normalized.output_tokens       * catalog.usd_per_mtok_output
        + normalized.cache_read_tokens   * catalog.usd_per_mtok_cache_read
        + normalized.cache_write_tokens  * catalog.usd_per_mtok_cache_write
    ) / 1000000 as decimal(38, 10)) as est_cost_usd,

    -- the same tokens one tier down (null when there is no lower tier)
    cast((
        normalized.uncached_input_tokens * catalog.tier_down_usd_per_mtok_input
        + normalized.output_tokens       * catalog.tier_down_usd_per_mtok_output
        + normalized.cache_read_tokens   * catalog.tier_down_usd_per_mtok_cache_read
        + normalized.cache_write_tokens  * catalog.tier_down_usd_per_mtok_cache_write
    ) / 1000000 as decimal(38, 10)) as est_cost_tier_down_usd,

    -- the same input with no prompt caching at all (what caching saved / could save)
    cast((
        (normalized.uncached_input_tokens + normalized.cache_read_tokens + normalized.cache_write_tokens) * catalog.usd_per_mtok_input
        + normalized.output_tokens * catalog.usd_per_mtok_output
    ) / 1000000 as decimal(38, 10)) as est_cost_no_cache_usd
from normalized
left join catalog
    on normalized.destination_model = catalog.destination_model
