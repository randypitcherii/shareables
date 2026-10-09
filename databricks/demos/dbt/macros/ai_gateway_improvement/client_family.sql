{#-
  Normalize the client that sent a gateway request into one `client_family`.

  The gateway's own `session_metadata.coding_agent` wins when present. Older
  coding-agent builds never set it, so the user agent fills the gap: for
  example `claude-cli/2.1.269` with no coding_agent is still Claude Code.
  Order matters: the first match wins, coding agents before generic SDKs.

  Edit the list to teach the loop about your own clients. Keep it
  deterministic (no lookups): it runs inside a materialized view.
-#}
{% macro ai_gateway_client_family(coding_agent, user_agent) -%}
case
    when {{ coding_agent }} is not null and {{ coding_agent }} != '' then lower({{ coding_agent }})
    when {{ user_agent }} rlike '(?i)^claude-cli/'                then 'claude-code'
    when {{ user_agent }} rlike '(?i)^(codex|codex-tui|codex_exec)' then 'codex'
    when {{ user_agent }} rlike '(?i)cursor'                      then 'cursor'
    when {{ user_agent }} rlike '(?i)gemini-cli|geminicli'        then 'gemini'
    when {{ user_agent }} rlike '(?i)copilot'                     then 'github-copilot'
    when {{ user_agent }} rlike '(?i)^opencode'                   then 'opencode'
    when {{ user_agent }} rlike '(?i)^(cline|roo-code|kilo)'      then 'cline'
    when {{ user_agent }} rlike '(?i)^goose'                      then 'goose'
    when {{ user_agent }} rlike '(?i)^aider'                      then 'aider'
    when {{ user_agent }} rlike '(?i)^ucode/'                     then 'ucode'
    when {{ user_agent }} rlike '(?i)^genie[-_]'                  then 'genie-cli'
    when {{ user_agent }} rlike '(?i)^(DatabricksOpenAI|databricks-)' then 'databricks-sdk'
    when {{ user_agent }} rlike '(?i)^(OpenAI|AsyncOpenAI)/'      then 'openai-sdk'
    when {{ user_agent }} rlike '(?i)^(Anthropic|AsyncAnthropic)/' then 'anthropic-sdk'
    when {{ user_agent }} rlike '(?i)^(python-requests|python-httpx|Python-urllib|aiohttp)' then 'python-http'
    when {{ user_agent }} rlike '(?i)^Mozilla/'                   then 'browser'
    when {{ user_agent }} is null or {{ user_agent }} rlike '(?i)^unknown' then 'unknown'
    -- unknown client: keep its product token so new tools show up by name
    else coalesce(nullif(lower(substr(regexp_extract({{ user_agent }}, '^([^/ ]+)', 1), 1, 40)), ''), 'other')
end
{%- endmacro %}


{#- true for interactive coding-agent families (drives workload_type) -#}
{% macro ai_gateway_is_coding_agent(client_family) -%}
{{ client_family }} in (
    'claude-code', 'claude-desktop', 'codex', 'codex-desktop', 'cursor', 'gemini',
    'github-copilot', 'githubcopilotchat', 'opencode', 'cline', 'goose', 'aider', 'ucode', 'pi'
)
{%- endmacro %}


{#- first non-empty team tag, request tags before service tags -#}
{% macro ai_gateway_team(request_tags, service_tags) -%}
coalesce(
    {%- for key in var('ai_gateway_team_tag_keys') %}
    nullif({{ request_tags }}['{{ key }}'], ''),
    {%- endfor %}
    {%- for key in var('ai_gateway_team_tag_keys') %}
    nullif({{ service_tags }}['{{ key }}'], ''),
    {%- endfor %}
    cast(null as string)
)
{%- endmacro %}
